module Partitioned
  module BulkMethodsMixin
    class BulkUploadDataInconsistent < StandardError
      def initialize(model, table_name, expected_columns, found_columns, while_doing)
        super("#{model.name}: for table: #{table_name}; #{expected_columns} != #{found_columns}; #{while_doing}")
      end
    end
    #
    # BULK creation of many rows
    #
    # rows: an array of hashtables of data to insert into the database
    #       each hashtable must have the same number of keys (and same
    #       names for each key).
    #
    # options:
    #   :slice_size = 1000
    #   :returning = nil
    #   :check_consistency = true
    #
    # examples:
    #  first example didn't uses more options.
    #
    # rows = [{
    #   :name => 'Keith',
    #   :salary => 1000,
    # },
    # {
    #   :name => 'Alex',
    #   :salary => 2000,
    # }]
    #
    # Employee.create_many(rows)
    #
    #  this second example uses :returning option
    #  to returns key values
    #
    # rows = [{
    #   :name => 'Keith',
    #   :salary => 1000,
    # },
    # {
    #   :name => 'Alex',
    #   :salary => 2000,
    # }]
    #
    # options = {
    #   :returning => [:id]
    # }
    #
    # Employee.create_many(rows, options) returns [#<Employee id: 1>, #<Employee id: 2>]
    #
    #  third example uses :slice_size option.
    #  Slice_size - is an integer that specifies how many
    #  records will be created in a single SQL query.
    #
    # rows = [{
    #   :name => 'Keith',
    #   :salary => 1000,
    # },
    # {
    #   :name => 'Alex',
    #   :salary => 2000,
    # },
    # {
    #   :name => 'Mark',
    #   :salary => 3000,
    # }]
    #
    # options = {
    #   :slice_size => 2
    # }
    #
    # Employee.create_many(rows, options) will generate two insert queries
    #
    def create_many(rows, options = {})
      return [] if rows.blank?
      options[:slice_size] = 1000 unless options.has_key?(:slice_size)
      options[:check_consistency] = true unless options.has_key?(:check_consistency)
      returning_clause = ""
      if options[:returning]
        if options[:returning].is_a? Array
          returning_list = options[:returning].join(',')
        else
          returning_list = options[:returning]
        end
        returning_clause = " returning #{returning_list}"
      end
      returning = []

      created_at_value = Time.zone.now

      num_sequences_needed = rows.reject{|r| r[:id].present?}.length
      if num_sequences_needed > 0
        row_ids = connection.next_sequence_values(sequence_name, num_sequences_needed)
      else
        row_ids = []
      end
      rows.each do |row|
        # set the primary key if it needs to be set
        row[:id] ||= row_ids.shift
      end.each do |row|
        # set :created_at if need be
        row[:created_at] ||= created_at_value
      end.group_by do |row|
        respond_to?(:partition_name) ? partition_name(*partition_key_values(row)) : table_name
      end.each do |table_name, rows_for_table|
        column_names = rows_for_table[0].keys.sort{|a,b| a.to_s <=> b.to_s}
        sql_insert_string = "insert into #{table_name} (#{column_names.join(',')}) values "
        rows_for_table.map do |row|
          if options[:check_consistency]
            row_column_names = row.keys.sort{|a,b| a.to_s <=> b.to_s}
            if column_names != row_column_names
              raise BulkUploadDataInconsistent.new(self, table_name, column_names, row_column_names, "while attempting to build insert statement")
            end
          end
          column_values = column_names.map do |column_name|
            quote_value(row[column_name], columns_hash[column_name.to_s])
          end.join(',')
          "(#{column_values})"
        end.each_slice(options[:slice_size]) do |insert_slice|
          returning += find_by_sql(sql_insert_string + insert_slice.join(',') + returning_clause)
        end
      end
      return returning
    end

    #
    # BULK updates of many rows
    #
    # rows: an array of hashtables of data to insert into the database
    #       each hashtable must have the same number of keys (and same
    #       names for each key).
    #
    # options:
    #   :slice_size = 1000
    #   :returning = nil
    #   :set_array = from first row passed in
    #   :check_consistency = true
    #   :where = '"#{table_name}.id = datatable.id"'
    #
    # examples:
    #  this first example uses "set_array" to add the value of "salary"
    #  to the specific employee's salary
    #  the default where clause is to match IDs so, it works here.
    # rows = [{
    #   :id => 1,
    #   :salary => 1000,
    # },
    # {
    #   :id => 10,
    #   :salary => 2000,
    # },
    # {
    #   :id => 23,
    #   :salary => 2500,
    # }]
    #
    # options = {
    #   :set_array => '"salary = datatable.salary"'
    # }
    #
    # Employee.update_many(rows, options)
    #
    #
    #  this versions sets the where clause to match Salaries.
    # rows = [{
    #   :id => 1,
    #   :salary => 1000,
    #   :company_id => 10
    # },
    # {
    #   :id => 10,
    #   :salary => 2000,
    #   :company_id => 12
    # },
    # {
    #   :id => 23,
    #   :salary => 2500,
    #   :company_id => 5
    # }]
    #
    # options = {
    #   :set_array => '"company_id = datatable.company_id"',
    #   :where => '"#{table_name}.salary = datatable.salary"'
    # }
    #
    # Employee.update_many(rows, options)
    #
    #
    #  this version sets the where clause to the KEY of the hash passed in
    # and the set_array is generated from the VALUES
    #
    # rows = {
    #   { :id => 1 } => {
    #     :salary => 100000,
    #     :company_id => 10
    #   },
    #   { :id => 10 } => {
    #     :salary => 110000,
    #     :company_id => 12
    #   },
    #   { :id => 23 } => {
    #     :salary => 90000,
    #     :company_id => 5
    #   }
    # }
    #
    # Employee.update_many(rows)
    #
    # Remember that you should probably set updated_at using "updated = datatable.updated_at"
    # or "updated_at = now()" in the set_array if you want to follow
    # the standard active record model for time columns (and you have an updated_at column)

    def update_many(rows, options = {})
      return [] if rows.blank?
      if rows.is_a?(Hash)
        options[:where] = '"' + rows.keys[0].keys.map{|key| '#{table_name}.' + "#{key} = datatable.#{key}"}.join(' and ') + '"'
        options[:set_array] = '"' + rows.values[0].keys.map{|key| "#{key} = datatable.#{key}"}.join(',') + '"' unless options[:set_array]
        r = []
        rows.each do |key,value|
          r << key.merge(value)
        end
        rows = r
      end
      unless options[:set_array]
        column_names =  rows[0].keys
        columns_to_remove = [:id]
        columns_to_remove += [partition_keys].map{|k| k.to_sym} if respond_to?(:partition_keys)
        options[:set_array] = '"' + (column_names - columns_to_remove.flatten).map{|cn| "#{cn} = datatable.#{cn}"}.join(',') + '"'
      end
      options[:slice_size] = 1000 unless options[:slice_size]
      options[:check_consistency] = true unless options.has_key?(:check_consistency)
      returning_clause = ""
      if options[:returning]
        if options[:returning].is_a?(Array)
          returning_list = options[:returning].map{|r| '#{table_name}.' + r.to_s}.join(',')
        else
          returning_list = options[:returning]
        end
        returning_clause = "\" returning #{returning_list}\""
      end
      options[:where] = '"#{table_name}.id = datatable.id"' unless options[:where]

      returning = []

      rows.group_by do |row|
        respond_to?(:partition_name) ? partition_name(*partition_key_values(row)) : table_name
      end.each do |table_name, rows_for_table|
        column_names = rows_for_table[0].keys.sort{|a,b| a.to_s <=> b.to_s}
        rows_for_table.each_slice(options[:slice_size]) do |update_slice|
          datatable_rows = []
          update_slice.each_with_index do |row,i|
            if options[:check_consistency]
              row_column_names = row.keys.sort{|a,b| a.to_s <=> b.to_s}
              if column_names != row_column_names
                raise BulkUploadDataInconsistent.new(self, table_name, column_names, row_column_names, "while attempting to build update statement")
              end
            end
            datatable_rows << row.map do |column_name,column_value|
              column_name = column_name.to_s
              columns_hash_value = columns_hash[column_name]
              if i == 0 
                "#{quote_value(column_value, columns_hash_value)}::#{columns_hash_value.sql_type} as #{column_name}"
              else
                quote_value(column_value, columns_hash_value)
              end
            end.join(',')
          end
          datatable = datatable_rows.join(' union select ')

          sql_update_string = <<-SQL
            update #{table_name} set
              #{eval(options[:set_array])}
            from
            (select
              #{datatable}
            ) as datatable
            where
              #{eval(options[:where])}
            #{eval(returning_clause)}
          SQL
          returning += find_by_sql(sql_update_string)
        end
      end
      return returning
    end
  end
end
