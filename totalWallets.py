from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/deltax2016/olympus-wallets")
client = Client(transport=transport, fetch_schema_from_transport=True)

def getTotalWallets():

    queryString = f"""query getActivated {{
        totalSupply(id:0){{
            totalWallets
        }}
    }}
    """
    # balance before listing
    
    query = gql(queryString)

    result = client.execute(query)

    return result['totalSupply']['totalWallets']


res = getTotalWallets()

print(res)