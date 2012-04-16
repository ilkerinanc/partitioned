require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'

module ActiveRecord::ConnectionAdapters
  class TableDefinition
    def check_constraint(constraint)
      @columns << Struct.new(:to_sql).new("CHECK (#{constraint})")
    end
  end

  class PostgreSQLAdapter < AbstractAdapter
    #
    # Get the next value in a sequence. Used on INSERT operation for
    # partitioning like by_id because the ID is required before the insert
    # so that the specific child table is known ahead of time.
    #
    def next_sequence_value(sequence_name)
      return execute("select nextval('#{sequence_name}')").field_values("nextval").first
    end

    #
    # Get the some next values in a sequence.
    # batch_size - count of values.
    #
    def next_sequence_values(sequence_name, batch_size)
      result = execute("select nextval('#{sequence_name}') from generate_series(1, #{batch_size})")
      return result.field_values("nextval").map(&:to_i)
    end

    #
    # Causes active resource to fetch the primary key for the table (using next_sequence_value())
    # just before an insert. We need the prefetch to happen but we don't have enough information
    # here to determine if it should happen, so Relation::insert has been modified to request of
    # the ActiveRecord::Base derived class if it requires a prefetch.
    #
    def prefetch_primary_key?(table_name)
      return false
    end

    #
    # Creates a schema given a name.
    # options:
    #   :unless_exists - check if schema exists.
    #
    def create_schema(name, options = {})
      if options[:unless_exists]
        return if execute("select count(*) from pg_namespace where nspname = '#{name}'").getvalue(0,0).to_i > 0
      end
      execute("CREATE SCHEMA #{name}")
    end

    #
    # Drop a schema given a name.
    # options:
    #   :if_exists - check if schema exists.
    #   :cascade - cascade drop to dependant objects
    #
    def drop_schema(name, options = {})
      if options[:if_exists]
        return if execute("select count(*) from pg_namespace where nspname = '#{name}'").getvalue(0,0).to_i == 0
      end
      execute("DROP SCHEMA #{name}#{' cascade' if options[:cascade]}")
    end

    #
    # Add foreign key constraint to table.
    #
    def add_foreign_key(referencing_table_name, referencing_field_name, referenced_table_name, referenced_field_name = :id)
      execute("ALTER TABLE #{referencing_table_name} add foreign key (#{referencing_field_name}) references #{referenced_table_name}(#{referenced_field_name})")
    end
  end
end
