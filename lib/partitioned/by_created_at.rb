module Partitioned
  #
  # Partition tables by created_at grouping them by week, with
  # a week defined as seven days starting on Monday.
  #
  class ByCreatedAt < ByWeeklyTimeField
    self.abstract_class = true

    # the field to partition on, `created_at`
    # @return [Symbol] the partition field: `created_at`
    def self.partition_time_field
      return :created_at
    end
  end
end
