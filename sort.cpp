#include <fcntl.h>
#include <fmt/ostream.h>

#include <iostream>
#include <queue>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

namespace {
    seastar::logger lg("sort");
    static constexpr int record_size = 4096;
}  // namespace.

// Custom heap comparator for finding minimum during per-shard sorting phase.
// Value stored in heap denotes the index of the corresponding record in file.
// The stored value is divided by (run_size_ * record_size_) to get the run number or the index in buffer where the record is stored.
class PerShardHeapComparator {
 public:
  PerShardHeapComparator(std::vector<std::string>* buffer, int run_size,
               int record_size) : 
    buffer_(buffer),
    run_size_(run_size),
    record_size_(record_size){}
  bool operator()(const int& lhs, const int& rhs) const {
    return (*buffer_)[lhs / (run_size_ * record_size_)] >
           (*buffer_)[rhs / (run_size_ * record_size_)];
  }
  std::vector<std::string>* buffer_;
  int run_size_;
  int record_size_;
};

// Custom heap comparator for finding minimum during shard outputs combine phase.
// Value stored in heap denotes the cpu id. Since each shard delivers a sorted run. 
// Minimum from each sorted run is calculated and its cursor points to the next element in the sorted run. 
class CombinerHeapComparator {
 public:
  CombinerHeapComparator(std::vector<std::string>* buffer) :
    buffer_(buffer) {}
  bool operator()(const int& lhs, const int& rhs) const {
    return (*buffer_)[lhs] > (*buffer_)[rhs];
  }
  std::vector<std::string>* buffer_;
};

// Combines the output from all the shards into output file. Combining happens by 
// taking first element from all shards into memory, finding minimum using heap, and flushing it to disk, then bring next record from the pop'ed file to memory.
// This process is repeated till heap has any element left.
seastar::future<> combine_outputs(int record_size, const seastar::sstring& dir_path) {
  auto out_file = co_await seastar::open_file_dma(
      "/home/akj/assgn/output.txt",
      seastar::open_flags::create | seastar::open_flags::rw);
  int num_cpu = seastar::smp::count;
  std::vector<std::string> buffer(num_cpu);
  std::vector<seastar::file> files(num_cpu);
  std::vector<int> cursor(num_cpu, 0);
  std::vector<int> size(num_cpu);
  std::priority_queue<int, std::vector<int>, CombinerHeapComparator>
      indexed_heap(CombinerHeapComparator{&buffer});

  for (int i = 0; i < num_cpu; ++i) {
    std::string output_path =
        dir_path + "tmp/output" + std::to_string(i) + ".txt";
    ;
    size[i] = co_await seastar::file_size(output_path);
    files[i] =
        co_await seastar::open_file_dma(output_path, seastar::open_flags::ro);
    buffer[i].resize(record_size);
    auto in_size_unused =
        co_await files[i].dma_read<char>(0, buffer[i].data(), record_size);
    indexed_heap.push(i);
  }
  int pos = 0;
  while (!indexed_heap.empty()) {
    int top = indexed_heap.top();
    const char* x = buffer[top].c_str();  // O(1) time operation.
    auto out_size = co_await out_file.dma_write<char>(pos, x, record_size);
    indexed_heap.pop();
    cursor[top] += record_size;
    pos += record_size;
    if (cursor[top] < size[top]) {
      buffer[top].resize(record_size);
      auto s_size_unused = co_await files[top].dma_read<char>(
          cursor[top], buffer[top].data(), record_size);
      indexed_heap.push(top);
    }
  }
}


// Performs an iteration where records in the input file( which are already sorted in runs of run_size),
// are merged in batches of buffer_size and an output file is produced which has records sorted in runs of run_size*buffer_size.
// This process of iterations is expected to be called by caller until all the records are merged into a single run.
// It internally maintains an indexed_heap representing elements currently in memory. Heaps gets pop'ed and the corresponding element gets written to disk,
// and the next element of that run replaces the deleted element in buffer. This process keeps on repeating till heap has any element left.
seastar::future<> do_iteration(const std::string& input_path, int run_size,
                               int start_index, int end_index, int buffer_size,
                               int record_size, const char* output_path) {
  auto out_file = co_await seastar::open_file_dma(
      output_path, seastar::open_flags::create | seastar::open_flags::rw |
                       seastar::open_flags::truncate);
  auto in_file =
      co_await seastar::open_file_dma(input_path, seastar::open_flags::ro);
  std::vector<std::string> buffer(buffer_size);
  int i = start_index, pos = 0;
  while (i <= (int)end_index) {
    std::priority_queue<int, std::vector<int>, PerShardHeapComparator> indexed_heap(
        PerShardHeapComparator{&buffer, run_size, record_size});
    int start = i;
    for (; i <= (int)end_index &&
           i - start < buffer_size * run_size * record_size;
         i += (run_size * record_size)) {
      buffer[(i - start) / (run_size * record_size)].resize(record_size);
      auto in_size_unused = co_await in_file.dma_read<char>(
          i, buffer[(i - start) / (run_size * record_size)].data(),
          record_size);
      indexed_heap.push(i - start);
    }
    while (!indexed_heap.empty()) {
      int top = indexed_heap.top();
      const char* x = buffer[indexed_heap.top() / (run_size * record_size)]
                          .c_str();  // O(1) time operation.
      auto out_size = co_await out_file.dma_write<char>(pos, x, record_size);
      indexed_heap.pop();
      top += record_size;
      pos += record_size;
      if (top % (run_size * record_size) != 0 &&
          (top + start) <= (int)end_index) {
        buffer[top / (run_size * record_size)].resize(record_size);
        auto s_size = co_await in_file.dma_read<char>(
            top + start, buffer[top / (run_size * record_size)].data(),
            record_size);
        indexed_heap.push(top);
      }
    }
  }
  co_return;
}

// Main function which handles the shard division logic and shepherds the per-shard iteration as well as combine phases.
seastar::future<> external_sort(int buffer_size, seastar::sstring& dir_path) {
  co_await seastar::smp::invoke_on_all([=]() -> seastar::future<> {
    auto file_size = co_await seastar::file_size(dir_path + "input.txt");
    // lg.info("visdvf {} {}", seastar::sstring(dir_path + "input.txt"), dir_path);
    // lg.info("visdvf {} {} {}", seastar::sstring(dir_path + "input.txt"), dir_path, file_size);
    int run_size = 1, parity = 0, cpu_idx = seastar::this_shard_id(),
        num_cpu = seastar::smp::count;
    int record_per_shard = file_size / (num_cpu*record_size);
    int start = record_per_shard * cpu_idx * record_size,
        end = record_per_shard * (cpu_idx + 1) * record_size - 1;
    if (cpu_idx == num_cpu - 1) end = file_size - 1;
    std::string tmp_a_path =
        dir_path + "tmp/a" + std::to_string(cpu_idx) + ".txt";
    std::string tmp_b_path =
       dir_path + "tmp/b" + std::to_string(cpu_idx) + ".txt";
    std::string input_path = dir_path + "input.txt";
    std::string output_path =
        dir_path + "tmp/output" + std::to_string(cpu_idx) + ".txt";

    bool is_last = false;
    while (run_size < (file_size / record_size)) {
      parity = 1 - parity;
      if (run_size * buffer_size >= (file_size / record_size)) {
        is_last = true;
      }
      if (run_size == 1) {
        if (is_last)
          co_await do_iteration(input_path, run_size, start, end, buffer_size,
                                record_size, output_path.c_str());
        else
          co_await do_iteration(input_path, run_size, start, end, buffer_size,
                                record_size, tmp_a_path.c_str());
      } else if (!parity) {
        if (is_last)
          co_await do_iteration(tmp_a_path, run_size, 0, end - start,
                                buffer_size, record_size, output_path.c_str());
        else
          co_await do_iteration(tmp_a_path, run_size, 0, end - start,
                                buffer_size, record_size, tmp_b_path.c_str());
      } else {
        if (is_last)
          co_await do_iteration(tmp_b_path, run_size, 0, end - start,
                                buffer_size, record_size, output_path.c_str());
        else
          co_await do_iteration(tmp_b_path, run_size, 0, end - start,
                                buffer_size, record_size, tmp_a_path.c_str());
      }
      run_size *= buffer_size;
    }
    co_return;
  });

  co_await combine_outputs(record_size, dir_path);

  co_return;
}

// Testing function which just confirms that all the records present in final output are in lexicographical order.
seastar::future<bool> test_sort(const seastar::sstring& dir_path) {
  auto file = co_await seastar::open_file_dma(dir_path + "output.txt",
                                              seastar::open_flags::ro);
  auto file_size = co_await seastar::file_size(dir_path + "input.txt");
  std::string x, y;
  x.resize(record_size);
  y.resize(record_size);
  auto s_size = co_await file.dma_read<char>(0, x.data(), record_size);
  int parity = 0;
  for (int i = record_size; i < file_size; i += record_size) {
    if (!parity) {
      auto s_size = co_await file.dma_read<char>(i, y.data(), record_size);
      if (x > y) co_return false;
    } else {
      auto s_size = co_await file.dma_read<char>(i, x.data(), record_size);
      if (y > x) co_return false;
    }
    parity = 1 - parity;
  }
  co_return true;
}

int main(int argc, char** argv) {

  namespace bpo = boost::program_options;

  seastar::app_template::config app_cfg;
    app_cfg.name = "sort";

    seastar::app_template app(std::move(app_cfg));
    auto opt_add = app.add_options();
    opt_add
        ("buffer-size", bpo::value<int>()->required(), "Number of pages used by buffer per shard")
        ("dir-path", bpo::value<seastar::sstring>()->required(), "Path to the directory of input file");

  return app.run(argc, argv, [&]() -> seastar::future<int> {
    auto& configuration = app.configuration();
    int buffer_size = configuration["buffer-size"].as<int>();
    auto dir_path = configuration["dir-path"].as<seastar::sstring>();

    co_await external_sort(buffer_size, dir_path);
    auto ret = co_await test_sort(dir_path);
    if(ret){
        lg.info("Output is sorted.  SUCCESS!");
    } else {
        lg.info("Output is *not* sorted.  FAILURE!");
    }
    co_return 0;
  });
}