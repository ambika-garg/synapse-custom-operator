# this is the class which will become
# the super class of "Subclass" class
class Old():
	def hello():
		print("hello")

class Class(Old):
	def __init__(self, azure_synapse_workspace_dev_endpoint, azure_synapse_conn_id):
		self._conn = None
		self.conn_id = azure_synapse_conn_id
		self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
		super().__init__()


# this is the subclass of class "Class"
class SubClass(Class):
	# this is how we call super
	# class's constructor
	def __init__(self, azure_synapse_workspace_dev_endpoint: str, azure_synapse_conn_id: str):
		super().__init__(azure_synapse_workspace_dev_endpoint, azure_synapse_conn_id)
		self._async_conn = None
		self.conn_id = azure_synapse_conn_id
		print(self.conn_id)

# driver code
x = "Hello"
y = "World"

a = SubClass(x, y)
