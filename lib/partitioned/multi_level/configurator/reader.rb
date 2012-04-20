module Partitioned
  class MultiLevel
    module Configurator
      # coalesces and parses all {Data} objects allowing the
      # {PartitionManager} to request partitioning information froma
      # centralized source from multi level partitioned models
      class Reader < Partitioned::PartitionedBase::Configurator::Reader
        # configurator for a specific class level
        UsingConfigurator = Struct.new(:model, :sliced_class, :dsl)

        def initialize(most_derived_activerecord_class)
          super
          @using_classes = nil
          @using_configurators = nil
        end

        #
        # The field used to partition child tables.
        #
        # @return [Array<Symbol>] fields used to partition this model
        def on_fields
          unless @on_fields
            @on_fields = using_collect(&:on_field).map(&:to_sym)
          end
          return @on_fields
        end

        #
        # The schema name of the table who is the direct ancestor of a child table.
        #
        def parent_table_schema_name(*partition_key_values)
          if partition_key_values.length <= 1
            return super
          end

          return schema_name
        end

        #
        # The table name of the table who is the direct ancestor of a child table.
        #
        def parent_table_name(*partition_key_values)
          if partition_key_values.length <= 1
            return super
          end

          # [0...-1] is here because the base name for this parent table is defined by the remove the leaf key value
          # that is:
          # current top level table name: public.foos
          # child schema area: foos_partitions
          # current partition classes: ByCompanyId then ByCreatedAt
          # current key values:
          #   company_id: 42
          #   created_at: 2011-01-03
          # child table name: foos_partitions.p42_20110103
          # parent table: foos_partitions.p42
          # grand parent table: public.foos
          return parent_table_schema_name(*partition_key_values) + '.p' + base_name(*partition_key_values[0...-1])
        end

        #
        # Define the check constraint for a given child table.
        #
        def check_constraint(*partition_key_values)
          index = partition_key_values.length - 1
          value = partition_key_values[index]
          return using_configurator(index).check_constraint(value)
        end

        #
        # The name of the child table without the schema name or name prefix.
        #
        def base_name(*partition_key_values)
          parts = []
          partition_key_values.each_with_index do |value,index|
            parts << using_configurator(index).base_name(value)
          end
          return parts.join('_')
        end

        def using_configurator(index)
          return using_class(index).configurator
        end

        def using_class(index)
          return using_classes[index]
        end


        protected

        def using_configurators
          unless @using_configurators
            @using_configurators = []
            using_classes.each do |using_class|
              using_class.ancestors.each do |ancestor|
                next if ancestor.class == Module
                @using_configurators << UsingConfigurator.new(using_class, ancestor, ancestor::configurator_dsl) if ancestor::configurator_dsl
                break if ancestor == Partitioned::PartitionedBase
              end
            end
          end
          return @using_configurators
        end

        def using_classes
          unless @using_classes
            @using_classes = collect_from_collection(&:using_classes).inject([]) do |array,new_items|
              array += [*new_items]
            end.to_a
          end
          return @using_classes
        end

        def using_collect(*partition_key_values, &block)
          values = []
          using_configurators.each do |using_configurator|
            data = using_configurator.dsl.data
            intermediate_value = block.call(data) rescue nil
            if intermediate_value.is_a? Proc
              values << intermediate_value.call(using_configurator.model, *partition_key_values)
            elsif intermediate_value.is_a? String
              values << eval("\"#{intermediate_value}\"")
            else
              values << intermediate_value unless intermediate_value.blank?
            end
          end
          return values
        end

        def using_collect_first(*partition_key_values, &block)
          using_configurators.each do |using_configurator|
            data = using_configurator.dsl.data
            intermediate_value = block.call(data) rescue nil
            if intermediate_value.is_a? Proc
              return intermediate_value.call(using_configurator.model, *partition_key_values)
            elsif intermediate_value.is_a? String
              return eval("\"#{intermediate_value}\"")
            else
              return intermediate_value unless intermediate_value.nil?
            end
          end
          return nil
        end

        def using_collect_from_collection(*partition_key_values, &block)
          values = []
          using_configurators.each do |using_configurator|
            data = using_configurator.dsl.data
            intermediate_values = []
            intermediate_values = block.call(data) rescue nil
            [*intermediate_values].each do |intermediate_value|
              if intermediate_value.is_a? Proc
                values << intermediate_value.call(using_configurator.model, *partition_key_values)
              elsif intermediate_value.is_a? String
                values << eval("\"#{intermediate_value}\"")
              else
                values << intermediate_value unless intermediate_value.blank?
              end
            end
          end
          return values
        end

      end
    end
  end
end
