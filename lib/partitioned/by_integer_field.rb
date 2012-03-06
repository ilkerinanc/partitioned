module Partitioned
  class ByIntegerField < PartitionedBase
    self.abstract_class = true

    def self.partition_table_size
      return 1
    end

    def self.partition_integer_field
      raise MethodNotImplemented.new(self, :partition_integer_field)
    end

    def self.partition_normalize_key_value(integer_field_value)
      return integer_field_value / partition_table_size * partition_table_size
    end

    partitioned do |partition|
      partition.on lambda {|model| return model.partition_integer_field }

      partition.order "substring(tablename, 2)::integer desc"

      partition.check_constraint lambda { |model, id|
        value = model.partition_normalize_key_value(id)
        if model.partition_table_size == 1
          return "( #{model.partition_integer_field} = #{value} )"
        else
          return "( #{model.partition_integer_field} >= #{value} and #{model.partition_integer_field} < #{value + model.partition_table_size} )"
        end
      }
    end
  end
end
