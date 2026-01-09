import unittest
class SimpleTestCase(unittest.TestCase):
    def test_one(self):
        print("Test is running!")
        self.assertTrue(True)
if __name__ == '__main__':
    unittest.main()
