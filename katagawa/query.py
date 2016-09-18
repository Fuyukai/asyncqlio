
class BaseQuery(object):
    """
    A BaseQuery object is used to query the database with SELECT statements or otherwise.
    """
    def __init__(self, session, **kwargs):
        """
        Creates a new BaseQuery.

        :param session: The session to bind this query to.
        """
        self.session = session

        # Define a dict of tables to access in this query.
        self.tables = {}

        # Define a list of conditions to generate in the SELECT.
        conditions = []


