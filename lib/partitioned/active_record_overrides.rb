#
# these are things our base class must fix in ActiveRecord::Base
#
# no need to monkey patch these, just override them.
#
module Partitioned
  module ActiveRecordOverrides
    #
    # arel_attribute_values needs to return attributes (and their values) associated with the dynamic_arel_table instead of the
    # static arel_table provided by ActiveRecord.
    #
    # the standard release of this function gathers a collection of attributes and creates a wrapper function around them
    # that names the table they are associated with. that naming is incorrect for partitioned tables.
    #
    # we class the standard release's method then retrofit our partitioned table into the hash that is returned.
    #
    def arel_attributes_values(include_primary_key = true, include_readonly_attributes = true, attribute_names = @attributes.keys)
      attrs = super
      actual_arel_table = dynamic_arel_table(self.class.table_name)
      return Hash[*attrs.map{|k,v| [actual_arel_table[k.name], v]}.flatten]
    end

    #
    # delete just needs a wrapper around it to specify the specific partition.
    #
    def delete
      if persisted?
        self.class.from_partition(id).delete(id)
      end
      @destroyed = true
      freeze
    end
  end
end
