require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/relation.rb'
require 'active_record/connection_adapters/abstract/connection_pool'

#
# patching activerecord to allow specifying the table name as a function of
# attributes
#
module ActiveRecord
  #
  # patches for relation to allow back hooks into the activerecord
  # requesting name of table as a function of attributes
  #
  class Relation
    #
    # patches activerecord's building of an insert statement to request
    # of the model a table name with respect to attribute values being
    # inserted
    #
    # the differences between this and the original code are small and marked
    # with PARTITIONED comment
    def insert(values)
      primary_key_value = nil

      if primary_key && Hash === values
        primary_key_value = values[values.keys.find { |k|
          k.name == primary_key
        }]

        if !primary_key_value && connection.prefetch_primary_key?(klass.table_name)
          primary_key_value = connection.next_sequence_value(klass.sequence_name)
          values[klass.arel_table[klass.primary_key]] = primary_key_value
        end

        #
        # PARTITIONED ADDITION.  prefetch_primary_key as requested by class.
        #
        if !primary_key_value && @klass.respond_to?(:prefetch_primary_key?)
          primary_key_value = connection.next_sequence_value(klass.sequence_name)
          values[klass.arel_table[klass.primary_key]] = primary_key_value
        end
      end

      im = arel.create_insert
      #
      # PARTITIONED ADDITION. get arel_table from class with respect to the
      # current values to placed in the table (which hopefully hold the values
      # that are used to determine the child table this insert should be
      # redirected to)
      #
      puts "***"
      puts Hash[*values.map{|k,v| [k.name,v]}.flatten].inspect
      puts "@@@"
      actual_arel_table = @klass.dynamic_arel_table(Hash[*values.map{|k,v| [k.name,v]}.flatten]) if @klass.respond_to? :dynamic_arel_table
      actual_arel_table = @table unless actual_arel_table
      im.into actual_arel_table

      conn = @klass.connection

      substitutes = values.sort_by { |arel_attr,_| arel_attr.name }
      binds       = substitutes.map do |arel_attr, value|
        [@klass.columns_hash[arel_attr.name], value]
      end

      substitutes.each_with_index do |tuple, i|
        tuple[1] = conn.substitute_at(binds[i][0], i)
      end

      if values.empty? # empty insert
        im.values = Arel.sql(connection.empty_insert_statement_value)
      else
        im.insert substitutes
      end

      conn.insert(
        im,
        'SQL',
        primary_key,
        primary_key_value,
        nil,
        binds)
    end
  end

  module ConnectionAdapters
    class ConnectionHandler
      # Remove the connection for this class. This will close the active
      # connection and the defined connection (if they exist). The result
      # can be used as an argument for establish_connection, for easily
      # re-establishing the connection.
      #
      # NOTE: 2011-05-17 changed the connection pools key to come from
      # connection_pools_hash_key, to help avoid spawning lots of database
      # connection pools when using multiple databases.
      def remove_connection(klass)
        #pool = @connection_pools[klass.name]
        pool_key = connection_pools_hash_key(klass)
        pool = @connection_pools[pool_key]
        @connection_pools.delete_if { |key, value| value == pool }
        pool.disconnect! if pool
        pool.spec.config if pool
      end

      # NOTE: 2011-05-17 changed the connection pools key to come from
      # connection_pools_hash_key, to help avoid spawning lots of database
      # connection pools when using multiple databases.
      def retrieve_connection_pool(klass)
        #pool = @connection_pools[klass.name]
        pool_key = connection_pools_hash_key(klass)
        pool = @connection_pools[pool_key]

        return pool if pool
        return nil if ActiveRecord::Base == klass
        retrieve_connection_pool klass.superclass
      end

      # Returns the value of the class's connection_pools_hash_key method if
      # there is one, defaulting to the class name.
      # 
      # Ordinarily, ActiveRecord has trouble dealing with connection pools
      # when using multiple databases. ActiveRecord will normally recursively
      # walk up the inheritance chain until it finds an existing connection
      # pool, or reaches ActiveRecord::Base (see retrieve_connection_pool).
      # If an application uses multiple databases, for each database other 
      # than the default one that ActiveRecord::Base connects to, all models
      # using that database should thereford descend from a single base class
      # that calls establish_connection; otherwise, Rails will create a
      # separate connection pool for each model that calls establish_connection,
      # potentially resulting in many more database connections than necessary.
      # 
      # This paradigm can be problematic for models defined in your application
      # that don't descend directly from ActiveRecord::Base.
      # 
      # With the fix below, when such models specify a 
      # self.connection_pools_hash_key method, the return value of that
      # will be inspected when searching for a connection pool in the hash.
      # All models that share the same return value from
      # connection_pools_hash_key will then share a single connection pool.
      def connection_pools_hash_key(klass)
        if klass.respond_to?(:connection_pools_hash_key)
          return klass.connection_pools_hash_key
        else
          return klass.name
        end
      end
    end
  end
end
