module Partitioned
  #
  # partition tables by a time field grouping them by week, with
  # a week defined as seven days starting on Monday.
  #
  class ByMonthlyTimeField < ByTimeField
    self.abstract_class = true

    def self.partition_normalize_key_value(time_value)
      return time_value.at_beginning_of_month
    end

    def self.partition_table_size
      return 1.month
    end

    partitioned do |partition|
      partition.base_name lambda { |model, time_field|
        return model.partition_normalize_key_value(time_field).strftime('%Y%m')
      }
    end
  end
end
