module CsvUtils
  class CsvProcessor

    attr_reader :file_path, :table
    attr_writer :file_path
    
    def initialize *args
      options = args.extract_options!
      @options = options
      @file_name = options[:file_name]
      @file_name_no_format = CsvUtils::CsvProcessor.format_table_name(options[:file_name])
      @file_path = options[:file_path]
      @mode = options[:mode]
      @options[:table_name] = @file_name_no_format
      @parser = CsvParser.new(@options)
      @csv_job_id = options[:csv_job_id]
      @is_persistant = options[:persistant]
      get_auto_generated_table
    end

    def self.format_table_name file_name
      table_name = file_name.split('.')[0]
      table_name = CsvUtils::CsvParser.format_name(table_name).downcase
      return table_name
    end

    def validate_csv_file
      supported_types = ["csv", "tsv", "txt"]
      filename = @file_name.split('.')
      if(supported_types.include? filename[filename.length - 1])
        return true
      else
        return false
      end
    end
    
    def get_auto_generated_table
      @table = @parser.generate_table(:headers => get_headers,
                                      :persistant => @is_persistant)
    end

    def get_table_types
      get_auto_generated_table
      return @table.get_types_for_display
    end

    def create_table?
      tables = CsvUploadsDb::Base.connection.tables
      if(tables.include? @file_name_no_format)
        return false
      else
        return true
      end
    end

    def create_table table_type
      get_auto_generated_table
      query = ""
      if(table_type == "generated")
        query = @table.get_create_table
      else
        query = @table.get_all_string_table_schema
      end
      Csv.execute_sql(query)
    end

    def load_to_table
      csv = Csv.find_by_id(@csv_job_id)
      options = {
        :table_name => @file_name_no_format,
        :csv_job_id => @csv_job_id,
        :headers => get_headers,
        :parser => @parser,
        :reformat_dates => false,
        :table => @table,
        :persistant => @is_persistant
      }
      if(csv.date_format != "none")
        options[:reformat_dates] = true
        options[:date_format] = csv.date_format
      end
      @importer = CsvImporter.new(options)
      @importer.load_csv_to_table
    end

    def send_completed_email csv_job_id
      csv = Csv.find(csv_job_id)
      result = CsvImporter.get_sample_rows_from_table(csv.target_table_name,csv_job_id)
      Notifier.successful_csv_load(csv.user,result,csv.target_table_name).deliver
    end

    def send_failed_email (csv_job_id, error)
      csv = Csv.find(csv_job_id)
      Notifier.failed_csv_load(csv_job_id,error,csv.target_table_name).deliver
    end

    def self.failed_email (csv_job_id, error)
      csv = Csv.find(csv_job_id)
      Notifier.failed_csv_load(csv_job_id,error,csv.target_table_name).deliver
    end

    def get_csv_sample_data
      data = Hash.new
      data[:headers] = get_headers
      data[:rows] = get_sample_rows
      return data
    end
    
    def get_headers
      return @parser.get_headers
    end
    
    def get_sample_rows
      return @parser.get_sample_rows
    end

  end
end
