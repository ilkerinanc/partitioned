module Partitioned
  class MultiLevel
    module Configurator
      # partitioning configuration information
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
