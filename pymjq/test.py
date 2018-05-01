from pymongo import MongoClient
from jobqueue import JobQueue
import unittest

host = 'localhost'
port = 27017
pair = '%s:%d' % (host, port)


class TestJobQueue(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        client = MongoClient(host, port)
        client.pymongo_test.jobqueue.drop()
        cls.db = client.pymongo_test

    def tearDown(self):
        self.db['jobqueue'].drop()

    def test_init(self):
        jq = JobQueue(self.db)
        self.assertTrue(jq.valid())
        self.assertRaises(Exception, jq._create)

    def test_valid(self):
        jq = JobQueue(self.db)
        jq.db['jobqueue'].drop()
        jq._create(capped=False)
        self.assertFalse(jq.valid())
        self.assertRaises(Exception, jq._create)

    def test_publish(self):
        jq = JobQueue(self.db)
        job = {'message': 'hello world!'}
        jq.pub(job)
        self.assertEquals(jq.queue_count(), 1)
        jq.clear_queue()
        jq.q = None  # erase the queue
        self.assertRaises(Exception, jq.pub, job)

    def test_next(self):
        jq = JobQueue(self.db)
        self.assertRaises(Exception, jq.next)
        job = {'message': 'hello world!'}
        jq.pub(job)
        row = jq.next()
        self.assertEquals(row['data']['message'], 'hello world!')
        self.assertEquals(jq.queue_count(), 0)

    def test_iter(self):
        NUM_JOBS = 3
        num_jobs_queued = [NUM_JOBS]
        def iterator_wait():
            num_jobs_queued[0] -= 1
            return num_jobs_queued[0] < 0
        jq = JobQueue(self.db, iterator_wait=iterator_wait)
        for ii in range(1, NUM_JOBS + 1):
            job = {'message': 'I am # ' + str(ii)}
            jq.pub(job)
        num_jobs_done = 0
        for job in jq:
            print job['data']['message']
            num_jobs_done += 1
        self.assertEquals(num_jobs_done, NUM_JOBS)


if __name__ == '__main__':
    unittest.main()
