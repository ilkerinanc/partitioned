Roman_array = [
               [ 1000000, '_M'  ],
               [  900000, '_C_M' ],
               [  500000, '_D'  ],
               [  400000, '_C_D' ],
               [  100000, '_C'  ],
               [   90000, '_X_C' ],
               [   50000, '_L'  ],
               [   40000, '_X_L' ],
               [   10000, '_X'  ],
               [    9000, '_I_X' ],
               [    5000, '_V'  ],
               [    1000, 'M'  ],
               [     900, 'CM' ],
               [     500, 'D'  ],
               [     400, 'CD' ],
               [     100, 'C'  ],
               [      90, 'XC' ],
               [      50, 'L'  ],
               [      40, 'XL' ],
               [      10, 'X'  ],
               [       9, 'IX' ],
               [       5, 'V'  ],
               [       4, 'IV' ],
               [       1, 'I'  ]
              ]

def to_roman(val)
  if val < 0 or val >= 5000000
    raise "out of range '#{val}'"
  else
    s = ""
    Roman_array.each { |pair|
      while val >= pair[0]
        s << pair[1]
        val -= pair[0]
      end
    }
    return s
  end
end 
