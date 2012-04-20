module Partitioned
  class PartitionedBase < ActiveRecord::Base
    # the configuration manager for partitioning.
    # it supports, the front-end UI (a DSL) using {Dsl}
    # state using {Data}
    # and a parser using {Reader}
    module Configurator
    end
  end
end
