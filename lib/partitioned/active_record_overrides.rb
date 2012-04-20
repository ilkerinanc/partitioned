#
# These are things our base class must fix in ActiveRecord::Base
#
# No need to monkey patch these, just override them.
#
module Partitioned
  #
  # methods that need to be override in an ActiveRecord::Base derived class so that we can support partitioning
  #
  module ActiveRecordOverrides
    #
    # arel_attribute_values needs to return attributes (and their values) associated with the dynamic_arel_table instead of the
    # static arel_table provided by ActiveRecord.
    #
    # The standard release of this function gathers a collection of attributes and creates a wrapper function around them
    # that names the table they are associated with. that naming is incorrect for partitioned tables.
    #
    # We call the standard releases method then retrofit our partitioned table into the hash that is returned.
    #
    # @param [Boolean] include_primary_key (true)
    # @param [Boolean] include_readonly_attributes (true)
    # @param [Boolean] attribute_names (@attributes.keys)
    # @return [Hash] hash of key value pairs associated with persistent attributes
    def arel_attributes_values(include_primary_key = true, include_readonly_attributes = true, attribute_names = @attributes.keys)
      attrs = super
      actual_arel_table = dynamic_arel_table(self.class.table_name)
      return Hash[*attrs.map{|k,v| [actual_arel_table[k.name], v]}.flatten]
    end

    #
    # Delete just needs a wrapper around it to specify the specific partition.
    #
    # @return [optional] undefined
    def delete
      if persisted?
        self.class.from_partition(*self.class.partition_key_values(attributes)).delete(id)
      end
      @destroyed = true
      freeze
    end
  end
end
