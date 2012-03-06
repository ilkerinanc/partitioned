module Partitioned
  class MultiLevel
    module Configurator
      class Data < Partitioned::PartitionedBase::Configurator::Data
        attr_accessor :using_classes

        def initialize
          super
          @using_classes = []
        end
      end
    end
  end
end
