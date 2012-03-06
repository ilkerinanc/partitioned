module Partitioned
  class MultiLevel
    module Configurator
      class Dsl < Partitioned::PartitionedBase::Configurator::Dsl
        class InvalidForMultiLevelPartitioning < StandardError
          def initialize(model, dsl_key, remedy)
            super("#{model.name}: '#{dsl_key}' is not valid for multi-level partitioning.  #{remedy}")
          end
        end

        attr_reader :data, :model

        def initialize(most_derived_activerecord_class)
          super(most_derived_activerecord_class, Partitioned::MultiLevel::Configurator::Data)
          @using_classes = []
        end

        #
        # Definition of classes which will be used at multi level partitioning.
        #
        def using_classes(*classes)
          data.using_classes += [*classes]
        end

        def on(*ignored)
          raise InvalidForMultiLevelPartitioning.new(model, :on, "the partitioned keyword 'using' is used to define multi-level partitioned tables.")
        end
        
      end
    end
  end
end
