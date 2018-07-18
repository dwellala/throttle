/*
Please find this generic queue implementation for the throttle problem and trading system supporting ThrottleHandler.
1. Added unit tests for testing the queue(RateLimitedQueue).
2. Create 2 participants to send messages to ThrottleHandler and implementation for getting those messages withing limited rate.
3. Applied priority for cancel order requests (PullOrder).
*/

#include <functional>
#include <cassert>
#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <chrono>
#include <vector>
#include <list>
#include <iostream>
#include <optional> // c++17
#include <random>

#define ENABLE_TESTS 0

namespace
{
/*
class implemention based on priority queue with a flexibility of custom comparator.
MT - safe. The queue able to handle messages until rejection limit when pushing messages into queue.
return empty optional when message queue is empty or processed messages are saturated wichin a given time unit.
*/
template <typename T
	, typename Compare = std::less<T>
	, typename TimeUnitType = std::chrono::seconds>
class RateLimitedQueue
{
public:
	/*
	valuesPerTimeUnit - limit of poping per given timeunit.
	rejectionLimit - maximum pushing limit can be queued.
	*/
	RateLimitedQueue(size_t valuesPerTimeUnit, size_t rejectionLimit)
		: m_valuesPerTimeUnit(valuesPerTimeUnit)
		, m_rejectionLimit(rejectionLimit)
	{}

	RateLimitedQueue(const RateLimitedQueue&) = delete;

	RateLimitedQueue& operator= (const RateLimitedQueue&) = delete;

	RateLimitedQueue(RateLimitedQueue&&) = default;

	RateLimitedQueue& operator= (RateLimitedQueue&&) = default;

	/*
	push values, MT-safe and returns true if succeeded.
	*/
	bool push(const T& value)
	{
		std::lock_guard<std::mutex> lock(m_mx);

		if (m_rejectionLimit < m_queue.size())
		{
			return false;
		}

		m_queue.push(value);
		return true;
	}

	/*
	return empty optional when message queue is empty or 
	processed messages are saturated wichin a given time unit.
	*/
	std::optional<T> pop()
	{
		// remove expired timepoints from this timepoint.
		// there might be a chance to process more if timepoint container was freed some
		auto now = std::chrono::steady_clock::now();
		while (!m_timePoints.empty()
			&& (now - m_timePoints.front() > TimeUnitType(1)))
		{
			m_timePoints.pop_front();
		}
		
		std::lock_guard<std::mutex> lock(m_mx);
		if (!m_queue.empty()
			&& m_timePoints.size() < m_valuesPerTimeUnit)
		{
			m_timePoints.push_back(now);
			auto value = m_queue.top();

			m_queue.pop();
			return value;
		}
		// queue is empty or processed values are staturated withing last unit of time.
		return {};
	}
	
private:
	const size_t m_valuesPerTimeUnit, m_rejectionLimit;

	std::mutex m_mx;
	std::priority_queue<T, std::vector<T>, Compare> m_queue;

	std::list<std::chrono::steady_clock::time_point> m_timePoints;
};

// message definitions
// priority takes same order with this arragement
enum class OrderType { NEW, AMEND, PULL };

struct BaseType
{
	virtual ~BaseType() {}

	OrderType getType() const
	{
		return m_type;
	}
protected:
	OrderType m_type;
};

struct NewOrder : BaseType
{
	NewOrder()
	{
		m_type = OrderType::NEW;
	}

	char id_[12];
	char side_; //'B' for Buy 'S' for Sell
	int price_;
	int size_;
	char symbol_[10];
};

struct AmendOrder : BaseType
{
	AmendOrder()
	{
		m_type = OrderType::AMEND;
	}

	char id_[12];
	int price_;
	int size_;
};

struct PullOrder : BaseType
{
	PullOrder()
	{
		m_type = OrderType::PULL;
	}

	char id_[12];
};

struct Comparator
{
	bool operator()(std::shared_ptr<BaseType> left, std::shared_ptr<BaseType> right) const
	{
		return left->getType() < right->getType();
	}
};

// this is the useage for trading system.
class ThrottleHandler
{
	static const size_t s_limitPerSecond = 100;
	static const size_t s_rejectionLimit = 100000;

public:
	ThrottleHandler()
		: m_msgQueue(s_limitPerSecond, s_rejectionLimit)
	{
	}

	void send(std::shared_ptr<BaseType> msg)
	{
		m_msgQueue.push(msg);
	}

	void get(std::function<void(std::shared_ptr<BaseType>)> callback)
	{
		// @todo since we expect 100 msgs per sec, this might going into busy loop.
		// have to implement, a signaling mechanism.
		while (!m_stopped)
		{
			if (auto optMsg = m_msgQueue.pop())
				callback(*optMsg);
		}
	}

	void terminate()
	{
		m_stopped = true;
	}

private:
	RateLimitedQueue<std::shared_ptr<BaseType>, Comparator> m_msgQueue;
	bool m_stopped = false;
};
}

#if ENABLE_TESTS
// tests for testing RateLimitedQueue
namespace testQueue
{

template <typename T>
std::unique_ptr<std::thread> sendMessages(T& container, int count)
{
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> dis(1, 3);
	return std::make_unique<std::thread>([&]()
	{
		for (int i = 0; i < count; ++i)
			container.push(dis(gen));
	});
}

template <typename T>
std::chrono::seconds expectMessages(T& container, size_t count)
{
	size_t receivedCount = 0;
	auto initTime = std::chrono::steady_clock::now();
	while (true)
	{
		// pop messages
		if (container.pop())
			++receivedCount;
		if (receivedCount == count)
			break;
	}

	return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - initTime);

}

void push1000MessagesAndPop100withinNumberOfMilleseconds()
{
	RateLimitedQueue<int> queue(100, 10000); // rate-100msg/sec rejection_limit-10000

	auto t = sendMessages(queue, 1000);

	auto duration = expectMessages(queue, 100);
	std::cout << duration.count() << std::endl;
	assert(duration < std::chrono::seconds(1));

	t->join();		
}
	
void push1000MessagesAndPop101withinSecond()
{
	RateLimitedQueue<int> queue(100, 10000); // rate-100msg/sec rejection_limit-10000

	auto t = sendMessages(queue, 1000);

	auto duration = expectMessages(queue, 101);
	std::cout << duration.count() << std::endl;
	assert(duration == std::chrono::seconds(1));

	t->join();
}

void push1000MessagesAndPop1000Than10Second()
{
	RateLimitedQueue<int> queue(100, 10000); // rate-100msg/sec rejection_limit-10000

	auto t = sendMessages(queue, 1000);

	auto duration = expectMessages(queue, 1000);
	std::cout << duration.count() << std::endl;
	assert(duration < std::chrono::seconds(10));

	t->join();
}

void push1001MessagesAndPopAllwithin10Second()
{
	RateLimitedQueue<int> queue(100, 10000); // rate-100msg/sec rejection_limit-10000

	auto t = sendMessages(queue, 1001);

	auto duration = expectMessages(queue, 1001);
	std::cout << duration.count() << std::endl;
	assert(duration == std::chrono::seconds(10));

	t->join();
}
}
#endif

int main()
{
#if ENABLE_TESTS // enable tests for rateLimitedQueue
	testQueue::push1000MessagesAndPop100withinNumberOfMilleseconds();
	testQueue::push1000MessagesAndPop101withinSecond();
	testQueue::push1000MessagesAndPop1000Than10Second();
	testQueue::push1001MessagesAndPopAllwithin10Second();
#else // usage for trading system

	// 1. create throttle handler
	ThrottleHandler th;

	// 2. send 3 types of messages to throttle_handler by a participant
	std::thread participant1([&th]()
	{
		for (int i = 0; i < 1001; ++i)
		{
			th.send(std::make_shared<NewOrder>());
			th.send(std::make_shared<AmendOrder>());
			th.send(std::make_shared<PullOrder>());
		}
	});

	std::thread participant2([&th]()
	{
		for (int i = 0; i < 1000; ++i)
		{
			th.send(std::make_shared<NewOrder>());
			th.send(std::make_shared<AmendOrder>());
			th.send(std::make_shared<PullOrder>());
			std::this_thread::sleep_for(std::chrono::microseconds(1));
		}
	});

	// 3. wait for 2001 messages, within 20 sec
	std::cout << "2001 msgs sent by 2 participants\ngetting rate is 100msgs/sec\n\nwait.." << std::endl;
	auto initTime = std::chrono::steady_clock::now();
	int msgCount = 0;
	th.get([&msgCount, &th](std::shared_ptr<BaseType> msg)
	{
		if (++msgCount == 2001)
		{
			th.terminate();
		}
	});

	std::cout << std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::steady_clock::now() - initTime).count() << "sec took for getting all messages"<< std::endl;

	participant1.join();
	participant2.join();
#endif
	system("pause");
	return 0;
}


