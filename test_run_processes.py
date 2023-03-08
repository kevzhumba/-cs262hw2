import unittest
from io import StringIO
from run_processes import *
import queue
from unittest.mock import MagicMock, mock_open, patch

TEST_HOST = "127.0.0.1"
TEST_PORT1 = 6000
TEST_PORT2 = 6001
TEST_PORT3 = 6002


class ProcessesTest(unittest.TestCase):
    def setUp(self):
        self.stub = MagicMock()
        self.machine = Machine(
            TEST_HOST, 1, TEST_PORT1, [TEST_PORT2, TEST_PORT3])
        self.machine.set_msg_queue(queue.Queue())
        random.seed(1234)

    def test_consume_message_success(self):
        '''Make sure the consumer function correctly decodes message and adds it to the queue'''
        mock_conn = MagicMock()
        test_msg = "10"
        mock_conn.recv.return_value = f"{test_msg}\n".encode('ascii')
        self.machine._consume_one_msg(mock_conn)
        msg = self.machine.msg_queue.get_nowait()
        self.assertEqual(msg, test_msg)

    def test_execute_producer_success(self):
        '''Make sure the producer function correctly opens a log file and writes to it'''
        self.machine.set_msg_queue(queue.Queue())
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        with patch('run_processes.open', new=mock_open()) as _file, patch("time.sleep", side_effect=InterruptedError):
            self.machine._execute_producer()

            _file.assert_called_once_with(
                f"logs/{FOLDER}/trial{TRIAL}_machine_{self.machine.machine_id}.log", 'w')
            _file().write.assert_called()
            _file().flush.assert_called()

    def test_execute_producer_cycle_with_msg_success(self):
        '''
        Make sure the producer cycle function correctly reads from the message queue, updates clock, and writes to the log file
        '''
        self.machine.set_msg_queue(queue.Queue())
        msg_clock = 100
        self.machine.msg_queue.put(str(msg_clock))
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        file = MagicMock()
        clock = 5
        updated_clock = self.machine._execute_one_cycle(clock, file)

        file.write.assert_called()
        self.assertEquals(updated_clock, msg_clock+self.machine.rate)

    def test_execute_producer_cycle_without_msg_success(self):
        '''
        Make sure the producer cycle function correctly updates clock, and writes to the log file
        '''
        self.machine.set_msg_queue(queue.Queue())
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        file = MagicMock()
        clock = 5
        updated_clock = self.machine._execute_one_cycle(clock, file)

        file.write.assert_called()
        self.assertEquals(updated_clock, clock + self.machine.rate)

    def test_do_msg_op1(self):
        '''Make sure the do event function correctly sends a message to the first machine for op 1'''
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        file = MagicMock()
        op = 1
        clock = 5
        updated_clock = self.machine.do_event(
            op, clock, self.machine.other_machine_sockets, file)

        mock_socket1.send.assert_called_once_with(
            f"{str(clock)}\n".encode('ascii'))
        file.write.assert_called()
        file.flush.assert_called()
        self.assertEquals(updated_clock, clock + 1)

    def test_do_msg_op2(self):
        '''Make sure the do event function correctly sends a message to the second machine for op 2'''
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        file = MagicMock()
        op = 2
        clock = 5
        updated_clock = self.machine.do_event(
            op, clock, self.machine.other_machine_sockets, file)

        mock_socket2.send.assert_called_once_with(
            f"{str(clock)}\n".encode('ascii'))
        file.write.assert_called()
        file.flush.assert_called()
        self.assertEquals(updated_clock, clock + 1)

    def test_do_msg_op3(self):
        '''Make sure the do event function correctly sends a message to both machines for op 3'''
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        file = MagicMock()
        op = 3
        clock = 5
        updated_clock = self.machine.do_event(
            op, clock, self.machine.other_machine_sockets, file)

        mock_socket1.send.assert_called_once_with(
            f"{str(clock)}\n".encode('ascii'))
        mock_socket2.send.assert_called_once_with(
            f"{str(clock)}\n".encode('ascii'))
        file.write.assert_called()
        file.flush.assert_called()
        self.assertEquals(updated_clock, clock + 1)

    def test_do_msg_op_other(self):
        '''
        Make sure the do event function correctly does to send message to other machines and updates clock for op other than 1, 2, 3
        '''
        mock_socket1 = MagicMock()
        mock_socket2 = MagicMock()
        self.machine.other_machine_sockets = [mock_socket1, mock_socket2]
        file = MagicMock()
        op = 5
        clock = 5
        updated_clock = self.machine.do_event(
            op, clock, self.machine.other_machine_sockets, file)

        mock_socket1.send.assert_not_called()
        mock_socket2.send.assert_not_called()
        file.write.assert_called()
        file.flush.assert_called()
        self.assertEquals(updated_clock, clock + 1)


if __name__ == '__main__':
    unittest.main()
