# this is the class which will become
# the super class of "Subclass" class
class Old():
	def hello():
		print("hello")

class ParentClass(Old):
	def __init__(self, azure_synapse_workspace_dev_endpoint, azure_synapse_conn_id):
		super().__init__()
		self._conn = None
		self.conn_id = azure_synapse_conn_id
		self.azure_synapse_workspace_dev_endpoint = azure_synapse_workspace_dev_endpoint
		

class SubClass(ParentClass):
	def __init__(self, azure_synapse_workspace_dev_endpoint: str, azure_synapse_conn_id: str):
		ParentClass.__init__(self, azure_synapse_workspace_dev_endpoint, azure_synapse_conn_id)
		self._async_conn = None
		self.conn_id = azure_synapse_conn_id
		print(self.conn_id)

# driver code
x = "Hello"
y = "World"

a = SubClass(x, y)
