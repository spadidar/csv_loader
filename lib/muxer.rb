module Muxer
  class Processor
    
    def initialize *args
      options = args.extract_options!
      @file_path = options[:file_path]
      @filename = @file_path.split("/").last
      @code_block = options[:code_block]
      @max_processes = options[:max_processes].nil? ? 5 : options[:max_processes] 
      @split_size = options[:split_size].nil? ? "1000" : options[:split_size]
      @split_prefix =  Time.now.to_i.to_s + "_" + rand(1000).to_s
      @temp_dir = options[:temp_dir].nil? ? "./" : options[:temp_dir]
      @temp_dir[-1,1] == "/" ? @temp_dir : (@temp_dir = @temp_dir + "/")
      @pids = Array.new
      @sleep_counter = 0
    end
    
    def start
      make_tmp_dir
      @files = split_file
      @files.delete_if{|filename| filename == "." || filename == ".." }
      @files.map! {|filename| @temp_dir + filename }
      @files.reverse!
      results = Parallel.map(@files, :in_processes => @max_processes){|file| @code_block.call(file)}
      remove_split_files
      # process
      return results
    end

    ##################### Below is Experimental (ignore)

    def make_tmp_dir
      if(!File.directory? @temp_dir)
        Dir.mkdir(@temp_dir)
      end
    end

    def split_file
      cmd = "cd #{@temp_dir}; split -l #{@split_size} #{@file_path} #{@split_prefix}"
      `#{cmd}`
      return Dir::entries(@temp_dir)
    end
    
    def process 
      begin
        while(job_exists?)
          if(can_spawn?)
            spawn_process @files.pop
          else
            busy_wait
          end
        end
      ensure
        # kill_all
        remove_split_files
      end # begin
    end
    
    def spawn_process file
      begin
        @pids << fork do
          # Process.setpriority(Process::PRIO_PROCESS, 0, 10) 
          @code_block.call(file)
        end
      rescue Exception => e
        @pids.each do |pid|
          puts "Exception happened in spawn_process, killing #{pid}"
          Process.kill("SIGKILL", pid)
        end
        raise Exception.new("Exception in TextProcessor \n Killed all Process! \n #{e.message} \n #{e.backtrace}")
      end
    end
    
    def job_exists?
      return @files.length > 0 ? true : false
    end

    def can_spawn?
      return @pids.length >= @max_processes ? false : true
    end

    def process_exists? pid
      if File.exists? "/proc/#{pid.to_s}" 
        return true 
      else
        return false
      end
    end

    def busy_wait
      pids = Process.waitall
      pids.each do |pid|
        p = pid[0]
        @pids.delete(p)
      end      
    end
    
    def go_to_sleep?
      return @sleep_counter >= 3 ? false : true
    end

    def sleep_wait
      puts "sleeping for 1 min"
      @sleep_counter += 1
      sleep 60 # sleep 1 min
    end

    def remove_split_files
      @files.each do |file|
        File.delete(file)
      end
    end

    def kill_all
      @pids.each do |pid|
        if(process_exists? pid)
          puts "killing pid " + pid.to_s
          Process.kill("SIGKILL", pid)
        end
      end      
    end

  end
end
