module Partitioned
  #
  # partition tables by a time field grouping them by day
  #
  class ByTimeField < PartitionedBase
    self.abstract_class = true

    #
    # generate an enumerable that represents all the dates between
    # start_date and end_date skipping step
    #
    # this can be used to calls that take an enumerable like create_infrastructure
    #
    def self.partition_generate_range(start_date, end_date, step = :default)
      step = partition_table_size if step == :default
      current_date = partition_normalize_key_value(start_date)
      dates = []
      while current_date <= end_date
        dates << current_date
        current_date += step
      end
      return dates
    end

    #
    # normalize the value to the current day
    #
    def self.partition_normalize_key_value(time_value)
      return time_value.to_date
    end

    #
    # the size of the partition, 1.day
    #
    def self.partition_table_size
      return 1.day
    end

    #
    # abstract -- implement in a derived clas.
    # the name of the time-related field we will use to partition child tables
    #
    def self.partition_time_field
      raise MethodNotImplemented.new(self, :partition_time_field)
    end

    partitioned do |partition|
      partition.on lambda {|model| model.partition_time_field}

      partition.index lambda {|model, time_field|
        return Configurator::Data::Index.new(model.partition_time_field, {})
      }

      partition.order 'tablename desc'

      partition.base_name lambda { |model, time_field|
        return model.partition_normalize_key_value(time_field).strftime('%Y%m%d')
      }
      partition.check_constraint lambda { |model, time_field|
        date = model.partition_normalize_key_value(time_field)
        return "#{model.partition_time_field} >= '#{date.strftime}' AND #{model.partition_time_field} < '#{(date + model.partition_table_size).strftime}'"
      }
    end
  end
end
