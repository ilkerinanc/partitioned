module Partitioned
  #
  # Table partitioning by a referenced id column which itself is partitioned
  # further weekly by a date column.
  # 
  class MultiLevel < PartitionedBase
    self.abstract_class = true

    #
    # Normalize the values for the each of using class.
    #
    def self.partition_normalize_key_value(values)
      normalized_values = []
      [*values].each_with_index do |value,index|
        normalized_values << configurator.using_class(index).partition_normalize_key_value(value)
      end
      return normalized_values
    end
  end
end
