#
# :include: ../../README
#
module Partitioned
  #
  # used by PartitionedBase class methods that must be overidden.
  #
  class MethodNotImplemented < StandardError
    def initialize(model, method_name, is_class_method = true)
      super("#{model.name}#{is_class_method ? '.' : '#'}#{method_name}")
    end
  end

  #
  # PartitionedBase
  # an ActiveRecord::Base class that can be partitioned.
  #
  # Uses a domain specific language to configure, see Partitioned::PartitionedBase::Configurator
  # for more information
  #
  # Extends Partitioned::BulkMethodsMixin to provide create_many and update_many
  #
  # Uses PartitionManager to manage creation of child tables
  #
  # Monkey patches some ActiveRecord routines to call back to this class when INSERT and UPDATE
  # statements are built (to determine the table_name with respect to values being inserted or updated)
  #
  class PartitionedBase < ActiveRecord::Base
    include ActiveRecordOverrides
    extend Partitioned::BulkMethodsMixin

    self.abstract_class = true

    #
    # returns an array of attribute names (strings) used to fetch the key value(s)
    # the determine this specific partition table.
    #
    def self.partition_keys
      return configurator.on_fields
    end

    #
    # the specific values for a partition of this active record's type which are defined by
    # #partition_keys
    #
    def self.partition_key_values(values)
      symbolized_values = values.symbolize_keys
      return self.partition_keys.map{|key| symbolized_values[key.to_sym]}
    end

    #
    # the name of the current partition table determined by this active records attributes that
    # define the key value(s) for the constraint check.
    #
    def partition_table_name
      return self.class.partition_name(*self.class.partition_keys.map{|attribute_name| attributes[attribute_name]})
    end

    #
    # normalize the value to be used for partitioning.  This allows, for instance, a class that partitions on
    # a time field to group the times by month.  An integer field might be grouped by every 10mil values, A
    # string field might be grouped by its first character.
    #
    def self.partition_normalize_key_value(value)
      return value
    end

    #
    # range generation provided for methods like created_infrastructure that need a set of partition key values
    # to operate on.
    #
    def self.partition_generate_range(start_value, end_value, step = 1)
      return Range.new(start_value, end_value).step(step)
    end

    #
    # return an instance of this partition table's table manager.
    #
    def self.partition_manager
      @partition_manager = self::PartitionManager.new(self) unless @partition_manager.present?
      return @partition_manager
    end

    #
    # return an instance of this partition table's sql_adapter (used by the partition manage to
    # create SQL statements)
    #
    def self.sql_adapter
      @sql_adapter = self::SqlAdapter.new(self) unless @sql_adapter.present?
      return @sql_adapter
    end

    #
    # in activerecord 3.0 we need to supply an Arel::Table for the key value(s) used
    # to determine the specific child table to access.
    #
    def self.dynamic_arel_table(values, as = nil)
      @arel_tables ||= {}
      key_values = self.partition_key_values(values)
      new_arel_table = @arel_tables[key_values]
      arel_engine_hash = {:engine => self.arel_engine}
      arel_engine_hash[:as] = as unless as.blank?
      new_arel_table = Arel::Table.new(self.partition_name(*key_values), arel_engine_hash)
      return new_arel_table
    end

    #
    # used by our active record hacks to supply an Arel::Table given this active record's
    # current attributes.
    #
    def dynamic_arel_table(as = nil)
      key_values = Hash[*self.class.partition_keys.map{|name| [name,read_attribute(name)]}.flatten]
      return self.class.dynamic_arel_table(key_values, as)
    end

    # :from_partition_scope is generally not used directly,
    # use helper self.from_partition so that the derived class
    # can be passed into :from_partition_scope
    scope :from_partition_scope, lambda { |target_class, *partition_field|
      {
        :from => "#{target_class.partition_name(*partition_field)} AS #{target_class.table_name}"
      }
    }

    #
    # real scope (uses #from_partition_scope).  This scope is used to target the
    # active record find() to a specific child table and alias it to the name of the
    # parent table (so activerecord can generally work with it)
    #
    # Use as:
    #
    #   Foo.from_partition_as(KEY).find(:first)
    #
    # where KEY is the key value(s) used as the check constraint on Foo's table.
    #
    # Because the scope is specific to a class (a class method) but unlike
    # class methods is not inherited, one  must use this form (#from_partition) instead
    # of #from_partition_scope to get the most derived classes specific active record scope.
    #
    def self.from_partition(*partition_field)
      from_partition_scope(self, *partition_field)
    end

    # :from_partitioned_without_alias_scope is generally not used directly,
    # use helper self.from_partitioned_without_alias so that the derived class
    # can be passed into :from_partitioned_without_alias_scope
    scope :from_partitioned_without_alias_scope, lambda { |target_class, *partition_field|
      {
        :from => target_class.partition_name(*partition_field)
      }
    }

    #
    # real scope (uses #from_partitioned_without_alias_scope). This scope is used to target the
    # active record find() to a specific child table. Is probably best used in advanced
    # activerecord queries when a number of tables are involved in the query.
    #
    # Use as:
    #
    #   Foo.from_partition(KEY).find(:all, :select => "*")
    #
    # where KEY is the key value(s) used as the check constraint on Foo's table.
    #
    # it's not obvious why :select => "*" is supplied.  note activerecord wants
    # to use the name of parent table for access to any attributes, so without
    # the :select argument the sql result would be something like:
    #
    #   SELECT foos.* FROM foos_partitions.pXXX
    #
    # which fails because table foos is not referenced.  using the scope #from_partition_as
    # is almost always the correct thing when using activerecord.
    #
    # Because the scope is specific to a class (a class method) but unlike
    # class methods is not inherited, one  must use this form (#from_partitioned_without_alias) instead
    # of #from_partitioned_without_alias_scope to get the most derived classes specific active record scope.
    #
    def self.from_partitioned_without_alias(*partition_field)
      from_partitioned_without_alias_scope(self, *partition_field)
    end

    #
    # return a object used to read configurator information
    #
    def self.configurator
      unless @configurator
        @configurator = self::Configurator::Reader.new(self)
      end
      return @configurator
    end

    #
    # yields an object used to configure the ActiveRecord class for partitioning
    # using the Configurator Domain Specific Language.
    # 
    # usage:
    #   partitioned do |partition|
    #     partition.on    :company_id
    #     partition.index :id, :unique => true
    #     partition.foreign_key :company_id
    #   end
    #
    def self.partitioned
      @configurator_dsl ||= self::Configurator::Dsl.new(self)
      yield @configurator_dsl
    end

    #
    # returns the configurator DSL object
    #
    def self.configurator_dsl
      return @configurator_dsl
    end

    partitioned do |partition|
      #
      # the schema name to place all child tables.
      #
      # by default this will be the table name of the parent class with a suffix "_partitions".
      # for a parent table name foos, that would be foos_partitions
      #
      partition.schema_name lambda {|model|
        return model.table_name + '_partitions'
      }

      #
      # the table name of the table who is the direct ancestor of a child table.
      # The child table is defined by the partition key values passed in.
      #
      # By default this is just the active record's notion of the name of the class.
      # Multi Level partitiong requires more work.
      #
      partition.parent_table_name lambda {|model, *partition_key_values|
        return model.table_name
      }

      #
      # the schema name of the table who is the direct ancestor of a child table.
      # The child table is defined by the partition key values passed in.
      #
      partition.parent_table_schema_name lambda {|model, *partition_key_values|
        # this should be a connection_adapter thing
        return "public"
      }

      #
      # the prefix for a child table's name.  This is typically a letter ('p') so that
      # the base_name of the table can be a number generated programtically from
      # the partition key values.
      #
      # for instance, a child table of the table 'foos' may be partitioned on
      # the column company_id whose value is 42.  specific child table would be named
      # 'foos_partitions.p42'
      #
      # the 'p' is the name_prefix because 'foos_partitions.42' is not a valid table name
      # (without quoting).
      #
      partition.name_prefix lambda {|model, *partition_key_values|
        return "p"
      }

      #
      # the child tables name without the schema name
      #
      partition.part_name lambda {|model, *partition_key_values|
        configurator = model.configurator
        return "#{configurator.name_prefix}#{configurator.base_name(*partition_key_values)}"
      }

      #
      # the full name of a child table defined by the partition key values
      #
      partition.table_name lambda {|model, *partition_key_values|
        configurator = model.configurator
        return "#{configurator.schema_name}.#{configurator.part_name(*partition_key_values)}"
      }

      #
      # the name of the child table without a schema name or prefix. this is used to
      # build child table names for multi-level partitions.
      #
      # for a table named foos_partitions.p42, this would be "42"
      #
      partition.base_name lambda { |model, *partition_key_values|
        return model.partition_normalize_key_value(*partition_key_values).to_s
      }
    end

    ##
    # :singleton-method: drop_partition_table
    # delegated to Partitioned::PartitionedBase::PartitionManager#drop_partition_table

    ##
    # :singleton-method: create_partition_table
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_partition_table

    ##
    # :singleton-method: add_partition_table_index
    # delegated to Partitioned::PartitionedBase::PartitionManager#add_partition_table_index

    ##
    # :singleton-method: add_references_to_partition_table
    # delegated to Partitioned::PartitionedBase::PartitionManager#add_references_to_partition_table

    ##
    # :method: create_partition_schema
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_partition_schema

    ##
    # :singleton-method: add_parent_table_rules
    # delegated to Partitioned::PartitionedBase::PartitionManager#add_parent_table_rules

    ##
    # :method: drop_old_partitions
    # delegated to Partitioned::PartitionedBase::PartitionManager#drop_old_partitions

    ##
    # :method: create_new_partitions
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_new_partitions

    ##
    # :method: drop_old_partition
    # delegated to Partitioned::PartitionedBase::PartitionManager#drop_old_partition

    ##
    # :method: create_new_partition
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_new_partition

    ##
    # :method: create_new_partition_tables
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_new_partition_tables

    ##
    # :method: create_infrastructure
    # delegated to Partitioned::PartitionedBase::PartitionManager#create_infrastructure

    ##
    # :method: partition_table_name
    # delegated to Partitioned::PartitionedBase::PartitionManager#partition_table_name

    ##
    # :method: partition_name
    # delegated to Partitioned::PartitionedBase::PartitionManager#partition_table_name

    extend SingleForwardable
    def_delegators :partition_manager, :drop_partition_table, :create_partition_table,
      :add_partition_table_index, :add_references_to_partition_table,
      :create_partition_schema, :add_parent_table_rules, :drop_old_partitions,
      :create_new_partitions, :drop_old_partition, :create_new_partition,
      :create_new_partition_tables, :create_infrastructure, :partition_table_name
    def_delegator :partition_manager, :partition_table_name, :partition_name
  end
end
