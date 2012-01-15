module CsvUtils
  class TypeChecker

    def initialize *args
      @types = Hash.new
      @types = {"TEXT" => 1,
        "VARCHAR(255)" => 2,
        "BIGINT" => 3,
        "FLOAT" => 4,
        "INTEGER" => 5,
        "DATETIME" => 6}
    end
    
    class << self
      def type? value
        if is_date?(value)
          return "DATETIME"
        elsif is_integer?(value)
          if(is_big_int? value)
            return "BIGINT"
          else
            return "INTEGER"
          end
        elsif value.is_a?(Float)
          return "FLOAT"
        elsif value.is_a?(String)
          if(is_text? value)
            return "TEXT"
          else
            return "VARCHAR(255)"
          end
        else #cant determine
          return "VARCHAR(255)"
        end
      end#method
      
      def is_date? value
        if(value.to_s.length <= 6)
          return false
        elsif(value.length > 20)
          return false
        end

        begin
          if(Date.parse(value))
            return true
          else
            return false
          end
        rescue Exception => exc
          return false
        end
      end
      
      def is_integer? value
        if(value =~ /^\d+$/)
          return true
        else
          return false
        end
      end
      
      def is_big_int? value
        if(value.is_a?(String))
          value = value.to_i
        end
        
        if(value > 4294967295)
          return true
        else
          return false
        end
      end
      
      def is_text? value
        if(value.length > 255)
          return true
        else
          return false
        end
      end
    end #self class end

    ##
    # Selects a type from the given hash
    # hash structure [type => occurences, type => occs ..]
    # returns the type (string)
    ##
    def pick_type_from(typeHash)
      if(is_single_type? typeHash)
        return first_element(typeHash)
      else
        return determine_type(typeHash)
      end
        
    end

    def determine_type(typeHash)
      priority = 100 #just a random number bigger than all priorities
      selectedType = ""
      typeHash.each do |type, value|
        if(type_priority(type) < priority)
          priority = type_priority(type)
          selectedType = type
        end
      end
      return selectedType
    end

    def is_single_type?(typeHash)
      if(typeHash.length == 1)
        return true
      end
    end

    def first_element(typeHash)
      typeHash.each do |type, value|
        return type
      end
    end

    def type_priority(type)
      return @types[type]
    end

  end
end
