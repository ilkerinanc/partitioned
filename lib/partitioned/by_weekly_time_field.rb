module Partitioned
  #
  # partition tables by a time field grouping them by week, with
  # a week defined as seven days starting on Monday.
  #
  class ByWeeklyTimeField < ByTimeField
    self.abstract_class = true

    #
    # normalize a partition key value by week.  We've picked
    # the begining of the week to key on, which is Monday.
    #
    def self.partition_normalize_key_value(time_value)
      return time_value.at_beginning_of_week
    end

    #
    # The size of the partition table, 7 days (1.week)
    # 
    def self.partition_table_size
      return 1.week
    end

    partitioned do |partition|
      partition.base_name lambda { |model, time_field|
        return model.partition_normalize_key_value(time_field).strftime('%Y%m%d')
      }
    end
  end
end
