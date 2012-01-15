module CsvUtils
  class CsvImporter
    
    def initialize *args
      options = args.extract_options!
      @parser = options[:parser]
      @table_name = options[:table_name]
      @csv_job_id = options[:csv_job_id]
      @headers = options[:headers]
      @reformat_dates = options[:reformat_dates]
      @table = options[:table]
      @tmp_csv_file_name = "tmp/#{Time.now.strftime("%d-%m-%Y_%H_%M").to_s}_#{@csv_job_id.to_s}.csv"
      @tmp_csv_file = FasterCSV.open(@tmp_csv_file_name, "w", :col_sep => "\t", :quote_char => '"')
      @is_persistant = options[:persistant]

      if(@reformat_dates)
        @date_format = options[:date_format]
      end
      @field_types = @table.get_types_for_display
      @insert_stm = get_insert_statement
      @headers << "active_row"
      @headers << "csv_job_id"
      @file_index = 0
    end

    def load_csv_to_table
      deactivate_all_older_rows unless @is_persistant

      @parser.iterate do |row_hash|
        row_hash["csv_job_id"] = @csv_job_id
        row_hash["active_row"] = "TRUE"
        if(@reformat_dates)
          row_hash = format_row(row_hash)
        end
        save_hash_to_csv(row_hash)
      end
      bulk_import_csv
      FileUtils.rm @tmp_csv_file_name if File.exists? @tmp_csv_file_name
    end
   
    def format_row hash
      hash.each do |header, value|
        if(@field_types[header] == "DATETIME")
          if(!value.nil?)
            formatted_value = parse_date(value, @date_format)
            hash.delete(header)
            hash[header] = formatted_value
          end
        end
      end
      return hash
    end

    def parse_date(date,format)
      if(format.length > 8)
        value = DateTime.strptime(date, format).to_s(:db)
        return value
      else
        value = Date.strptime(date, format).to_s(:db)
        return value
      end
    end

    def deactivate_all_older_rows
      Csv.execute_sql(get_deactivate_sql)
    end

    def insert_row row_hash
      insert_stm = @insert_stm  + "("
      counter = 0
      length = @headers.length - 1
      @headers.each do |header|
        if(!row_hash[header].nil?)
          insert_stm += "'" + Mysql.escape_string(row_hash[header].to_s) + "'"
        else
          insert_stm += "''"
        end
        if(counter < length)
          insert_stm += ","
        end
        counter += 1
      end 
      insert_stm += ")"

      Csv.execute_sql(insert_stm)
    end

    def bulk_import_csv
      options = {
        :file_name => @tmp_csv_file_name,
        :table => @table_name,
        :delimiter => '\t',
        :enclosed_by => '"',
        :lines => "\n"
      }
      if @is_persistant then options[:handle] = "REPLACE" end
      load_data_infile = BulkImport.get_load_data_infile options
      Kernel.p load_data_infile
      Csv.execute_sql load_data_infile
    end

    def save_hash_to_csv hash
      row = Array.new
      @headers.each do |header|
        row << hash[header]
      end
      save_to_csv row
    end
      
    def save_to_csv array
      @tmp_csv_file << array
      @tmp_csv_file.flush
    end

    def get_deactivate_sql
      stm = "UPDATE #{@table_name} SET active_row = 'FALSE'"
      return stm
    end

    def self.get_sample_rows_from_table(table,csv_job_id)
      query = "SELECT * FROM #{table} WHERE csv_job_id = #{csv_job_id} LIMIT 10"
      rows = Csv.execute_sql(query)
      results = Array.new
      results[0] = rows.fields
      rows.each do |row|
        results << row
      end

      return results
    end

    def get_insert_statement
      insert_stm = "INSERT INTO #{@table_name} ( "
      counter = 0
      @headers.each do |header|
        if(counter == @headers.length - 1)
          insert_stm += "`" + header + "`)"
        else
          insert_stm += "`" + header + "`,"
        end
        counter += 1
      end
      insert_stm += " VALUES "
      return insert_stm
    end
    
  end
end
