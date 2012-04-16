module Partitioned
  class MultiLevel
    class PartitionManager < Partitioned::PartitionedBase::PartitionManager
      #
      # The once called function to prepare a parent table for partitioning as well
      # as create the schema that the child tables will be placed in.
      #
      def create_infrastructure(enumerable = [[]])
        super()
        enumerable.each do |*partition_key_values|
          create_partition_schema(*partition_key_values)
        end
      end

      protected

      #
      # Create a specific child table that does not currently
      # exist and whose schema (the schema that the table exists in)
      # also already exists (#create_infrastructure is designed to
      # create this).
      #
      def create_new_partition(*partition_key_values)
        create_partition_table(*partition_key_values)
        if is_leaf_partition?(*partition_key_values)
          add_partition_table_index(*partition_key_values)  
          add_references_to_partition_table(*partition_key_values)
        else
          add_parent_table_rules(*partition_key_values)
        end
      end

      #
      # Is the table a child table without itself having any children.
      # generally leaf tables are where all indexes and foreign key
      # constraints will be placed because that is where the data will be.
      #
      # Non leaf tables will typically have a rule placed on them
      # (via add_parent_table_rules) that prevents any inserts from occurring
      # on them.
      #
      def is_leaf_partition?(*partition_key_values)
        return partition_key_values.length == parent_table_class.configurator.on_fields.length
      end
    end
  end
end
