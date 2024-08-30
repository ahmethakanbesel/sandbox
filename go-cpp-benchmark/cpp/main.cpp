#include <iostream>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <atomic>

std::atomic<unsigned long long> total_points_inside(0);

void estimate_pi(unsigned long long points_per_thread)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-1.0, 1.0);

    unsigned long long local_points_inside = 0;

    for (unsigned long long i = 0; i < points_per_thread; ++i)
    {
        double x = dis(gen);
        double y = dis(gen);
        if (x * x + y * y <= 1.0)
        {
            ++local_points_inside;
        }
    }

    total_points_inside += local_points_inside;
}

int main()
{
    const unsigned int num_threads = std::thread::hardware_concurrency();
    const unsigned long long total_points = 1000000000; // 1 billion points
    const unsigned long long points_per_thread = total_points / num_threads;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (unsigned int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back(estimate_pi, points_per_thread);
    }

    for (auto &t : threads)
    {
        t.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    double pi_estimate = 4.0 * total_points_inside / static_cast<double>(total_points);

    std::cout << "Estimated Pi: " << pi_estimate << std::endl;
    std::cout << "Time taken: " << diff.count() << " seconds" << std::endl;
    std::cout << "Threads used: " << num_threads << std::endl;

    return 0;
}