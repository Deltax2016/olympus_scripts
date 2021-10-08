from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/deltax2016/olympus-wallets")
client = Client(transport=transport, fetch_schema_from_transport=True)

def getActivated(timestamp_start, timestamp_end):

    queryString = f"""query getActivated {{
        wallets(where: {{birth_gt:{timestamp_start}, birth_lt: {timestamp_end}}}) {{
            address
        }}
    }}
    """
    # balance before listing
    
    query = gql(queryString)

    result = client.execute(query)

    return result

#wallet = "0x0822f3c03dcc24d200aff33493dc08d0e1f274a2"
timestamp_start = 1617291702
timestamp_end = 1617391702
res = getActivated(timestamp_start, timestamp_end)

print(res['wallets'])






