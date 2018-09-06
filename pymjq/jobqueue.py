import pymongo
from datetime import datetime
import time


class JobQueue:

    # Capped collection documents can not have its size updated
    # https://docs.mongodb.com/manual/core/capped-collections/#document-size
    DONE = 'done'.ljust(10, '_')
    WAITING = 'waiting'.ljust(10, '_')
    WORKING = 'working'.ljust(10, '_')

    def __init__(self, db, silent=False, iterator_wait=None, size=None, collection_name='jobqueue'):
        """ Return an instance of a JobQueue.
        Initialization requires one argument, the database,
        since we use one jobqueue collection to cover all
        sites in an installation/database. The second
        argument specifies if to print status while waiting
        for new job, the default value is False"""
        self.db = db
        self.collection_name = collection_name
        if not self._exists():
            print ('Creating "{}" collection.'.format(self.collection_name))
            self._create(size)
        self.q = self.db[self.collection_name]
        self.iterator_wait = iterator_wait
        if self.iterator_wait is None:
            def deafult_iterator_wait():
                if not silent:
                    print ('waiting!')
                time.sleep(5)
                return True

            self.iterator_wait = deafult_iterator_wait

    def _create(self, size=None, capped=True):
        """ Creates a Capped Collection. """
        try:
            # size - When creating a capped collection you must specify the maximum size
            #        of the collection in bytes, which MongoDB will pre-allocate for the
            #        collection. The size of the capped collection includes a small amount
            #        of space for internal overhead.
            # max - you may also specify a maximum number of documents for the collection
            if not size:
                size = 100000
            self.db.create_collection(self.collection_name,
                                      capped=capped,
                                      size=size)
        except:
            raise Exception('Collection "{}" already created'.format(self.collection_name))

    def _find_opts(self):
        if hasattr(pymongo, 'CursorType'):
            return {'cursor_type': pymongo.CursorType.TAILABLE_AWAIT}   # pylint: disable=no-member
        return {'Tailable': True}

    def _exists(self):
        """ Ensures that the jobqueue collection exists in the DB. """
        return self.collection_name in self.db.collection_names()

    def valid(self):
        """ Checks to see if the jobqueue is a capped collection. """
        opts = self.db[self.collection_name].options()
        if opts.get('capped', False):
            return True
        return False

    def next(self):
        """ Runs the next job in the queue. """
        cursor = self.q.find({'status': self.WAITING},
                             **self._find_opts())
        row = cursor.next()
        row = self.q.find_one_and_update({'_id': row['_id'],
                                          'status': self.WAITING},
                                         {'$set':
                                            {'status': self.DONE,
                                             'ts.started': datetime.utcnow(),
                                             'ts.done': datetime.utcnow()}})
        if row:
            return row
        raise Exception('There are no jobs in the queue')

    def pub(self, data=None):
        """ Publishes a doc to the work queue. """
        doc = dict(
            ts={'created': datetime.utcnow(),
                'started': datetime.utcnow(),
                'done': datetime.utcnow()},
            status=self.WAITING,
            data=data)
        try:
            self.q.insert(doc, manipulate=False)
        except:
            raise Exception('could not add to queue')
        return True

    def __iter__(self):
        """ Iterates through all docs in the queue
            and waits for new jobs when queue is empty. """
        cursor = self.q.find({'status': self.WAITING},
                             **self._find_opts())
        get_next = True
        while get_next:
            if not cursor.alive:
                cursor = self.q.find({'status': self.WAITING},
                                     **self._find_opts())
            try:
                row = cursor.next()
                row = self.q.find_one_and_update(
                    {'_id': row['_id'],
                     'status': self.WAITING},
                    {'$set':
                     {'status': self.WORKING,
                      'ts.started': datetime.utcnow()}})
                if row is None:
                    raise Exception('There are no jobs in the queue')
                print('---')
                print('Working on job:')
                yield row
                self.q.update_one({'_id': row['_id']},
                                  {'$set': {'status': self.DONE,
                                            'ts.done': datetime.utcnow()}})
            except StopIteration:
                get_next = self.iterator_wait()

    def queue_count(self):
        """ Returns the number of jobs waiting in the queue. """
        cursor = self.q.find({'status': self.WAITING})
        if cursor:
            return cursor.count()

    def clear_queue(self):
        """ Drops the queue collection. """
        self.q.drop()
