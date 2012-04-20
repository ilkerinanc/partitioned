module Partitioned
  class PartitionedBase
    module Configurator
      #
      # The state configured by the Dsl and read by Reader.
      #
      class Data
        # represents a SQL index
        class Index
          attr_accessor :field, :options
          def initialize(field, options = {})
            @field = field
            @options = options
          end
        end
        # represents a SQL foreign key reference
        class ForeignKey
          attr_accessor :referencing_field, :referenced_table, :referenced_field
          def initialize(referencing_field, referenced_table = nil, referenced_field = :id)
            @referencing_field = referencing_field
            @referenced_table = if referenced_table.nil? 
                                  self.class.foreign_key_to_foreign_table_name(referencing_field)
                                else
                                  referenced_table
                                end
            @referenced_field = referenced_field
          end

          #
          # Produce a table name from the name of the foreign key.  in rails, this really
          # means "foo_id" should be mapped to "foos", and "company_id" should be mapped to
          # "companies"
          #
          # @param [String] foreign_key_field the name of the foreign key field
          # @return [String] the name of the table associated with the foreign key
          def self.foreign_key_to_foreign_table_name(foreign_key_field)
            return ActiveSupport::Inflector::pluralize(foreign_key_field.to_s.sub(/_id$/,''))
          end
        end

        attr_accessor :on_field, :indexes, :foreign_keys, :last_partitions_order_by_clause,
           :schema_name, :name_prefix, :base_name,
           :part_name, :table_name, :parent_table_schema_name,
           :parent_table_name, :check_constraint, :encoded_name
        
        def initialize
          @on_field = nil
          @indexes = []
          @foreign_keys = []
          @last_partitions_order_by_clause = nil

          @schema_name = nil

          @name_prefix = nil
          @base_name = nil

          @part_name = nil

          @table_name = nil

          @parent_table_schema_name = nil
          @parent_table_name = nil

          @check_constraint = nil

          @encoded_name = nil
        end
      end
    end
  end
end
