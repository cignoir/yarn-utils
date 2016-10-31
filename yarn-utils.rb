# Usage: $ ruby yarn-utils.rb --help
# ex. $ ruby yarn-utils.rb -c 8 -m 61 -d 1 -k false

require 'optparse'

module YarnUtils
  class Profiler
    attr_reader :option
    attr_reader :cores, :memory, :disks, :hbase
    attr_accessor :reserved_memory, :reserved_stack_memory, :reserved_hbase_memory

    DEFAULT_OPTION = {cores: 16, memory: 64, disks: 4, hbase: true}
    # Reserved for OS + DN + NM,  Map: Memory => Reservation
    RESERVED_STACK = {4 => 1, 8 => 2, 16 => 2, 24 => 4, 48 => 6, 64 => 8, 72 => 8, 96 => 12, 128 => 24, 256 => 32, 512 => 64}.freeze
    # Reserved for HBase. Map: Memory => Reservation
    RESERVED_HBASE = {4 => 1, 8 => 1, 16 => 2, 24 => 4, 48 => 8, 64 => 8, 72 => 8, 96 => 16, 128 => 24, 256 => 32, 512 => 64}.freeze

    class << self
      def parse_option(argv)
        option = DEFAULT_OPTION
        OptionParser.new do |opt|
          opt.on('-c', '--cores VALUE', 'The number of cores on each host.') { |v| option[:cores] = v.to_i }
          opt.on('-m', '--memory VALUE', 'The amount of memory on each host in GB.') { |v| option[:memory] = v.to_i }
          opt.on('-d', '--disks VALUE', 'The number of disks on each host.') { |v| option[:disks] = v.to_i }
          opt.on('-k', '--hbase VALUE', '"true" if HBase is installed, "false" if not.') { |v| option[:hbase] = eval(v) }
          opt.parse!(argv)
        end
        option
      end

      def min_container_size(memory)
        case
          when memory <= 4 then 256
          when memory <= 8 then 512
          when memory <= 24 then 1024
          else 2048
        end
      end

      def reserved_stack_memory(memory)
        RESERVED_STACK[memory] || case
                                    when memory <= 4 then 1
                                    when memory >= 512 then 64
                                    else 1
                                  end
      end

      def reserved_hbase_memory(memory)
        RESERVED_HBASE[memory] || case
                                    when memory <= 4 then 1
                                    when memory >= 512 then 64
                                    else 2
                                  end
      end
    end

    def initialize(option = DEFAULT_OPTION)
      @option = option
      @cores, @memory, @disks, @hbase = @option[:cores], @option[:memory], @option[:disks], @option[:hbase]

      @min_container_size = Profiler.min_container_size(@memory)
      @reserved_stack_memory = Profiler.reserved_stack_memory(@memory)
      @reserved_hbase_memory = Profiler.reserved_hbase_memory(@memory)
    end

    def print
      puts "Using cores=#{@cores} memory=#{@memory}GB disks=#{@disks} hbase=#{@hbase}"

      reserved_memory = @reserved_stack_memory + (@hbase ? @reserved_hbase_memory : 0)
      usable_memory = @memory - reserved_memory
      free_memory = [usable_memory, 2].max * 1024
      reserved_memory = [0, free_memory - reserved_memory].max if usable_memory < 2

      puts "Profile: cores=#{@cores} memory=#{free_memory}MB reservedMem=#{reserved_memory}GB usableMem=#{usable_memory}GB disks=#{@disks}"

      containers = [3, [2 * @cores, [(1.8 * @disks.to_f).ceil, free_memory / @min_container_size].min].min.to_i].max
      container_ram = (free_memory / containers).abs
      container_ram = (container_ram / 512).floor.to_i * 512 if container_ram > 1024

      map_memory = container_ram
      reduce_memory = container_ram <= 2048 ? 2 * container_ram : container_ram
      am_memory = [map_memory, reduce_memory].max

      result = <<-RESULT
Num Container=#{containers}
Container Ram=#{container_ram} MB
Used Ram=#{(containers * container_ram / 1024.0).to_i}GB
Unused Ram=#{reserved_memory}GB
yarn.scheduler.minimum-allocation-mb=#{container_ram}
yarn.scheduler.maximum-allocation-mb=#{containers * container_ram}
yarn.nodemanager.resource.memory-mb=#{containers * container_ram}
mapreduce.map.memory.mb=#{map_memory}
mapreduce.map.java.opts=-Xmx#{(0.8 * map_memory).to_i}m
mapreduce.reduce.memory.mb=#{reduce_memory}
mapreduce.reduce.java.opts=-Xmx#{(0.8 * reduce_memory).to_i}m
yarn.app.mapreduce.am.resource.mb=#{am_memory}
yarn.app.mapreduce.am.command-opts=-Xmx#{(0.8 * am_memory).to_i}m
mapreduce.task.io.sort.mb=#{(0.4 * map_memory).to_i}
      RESULT

      puts result
    end
  end
end

option = YarnUtils::Profiler.parse_option(ARGV)
YarnUtils::Profiler.new(option).print
