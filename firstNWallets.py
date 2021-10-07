from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/deltax2016/olympus-wallets")
client = Client(transport=transport, fetch_schema_from_transport=True)

def getFirstWallets(number_of_periods=24,delta_per_period=3600,timestamp_start=None, cnt=None):
    timestamps = [timestamp_start-delta_per_period]

    for i in range(number_of_periods):
        timestamps.append( timestamp_start + i * delta_per_period )

    queryString = f"query balancesByWallet {{   wallets(orderBy: birth, first: {cnt}) {{ id "
    # balance before listing
    queryString += f"""t{timestamp_start-delta_per_period}:balances(first: 1, orderDirection: desc ,orderBy: timestamp, where:{{timestamp_lt: {timestamp_start}}}) {{
            id
            ohmBalance
            sohmBalance
            timestamp
        }}"""

    for i in range(1,len(timestamps)-1):
        queryString += f"""t{timestamps[i]}:balances(first: 1, orderDirection: desc ,orderBy: timestamp, where:{{timestamp_gte: {timestamps[i]}, timestamp_lt: {timestamps[i+1]}}}) {{
                id
                ohmBalance
                sohmBalance
                timestamp
            }}"""

    queryString += "}}"
    query = gql(queryString)

    result = client.execute(query)

    print(result['wallets'][0][f't{timestamp_start-delta_per_period}'])

    wallets = []
    for j in range(cnt):
        tempWallet = {}
        tempWallet['address'] = result['wallets'][j]['id']
        formatedData = []

        for i in range(len(timestamps)-1):
            tempObj = {}
            tempObj['timestampBegin'] = timestamps[i]
            tempObj['timestampEnd'] = timestamps[i+1]
            if not result['wallets'][j][f't{timestamps[i]}']:
                if i != 0:
                    tempObj['ohmBalance'] = formatedData[i-1]['ohmBalance']
                    tempObj['sohmBalance'] = formatedData[i-1]['sohmBalance']
                else:
                    tempObj['ohmBalance'] = 0
                    tempObj['sohmBalance'] = 0
            else:
                tempObj['ohmBalance'] = result['wallets'][j][f't{timestamps[i]}'][0]['ohmBalance']
                tempObj['sohmBalance'] = result['wallets'][j][f't{timestamps[i]}'][0]['sohmBalance']
            formatedData.append(tempObj)
        tempWallet['balances'] = formatedData
        wallets.append(tempWallet)
    return wallets

number_of_periods = 60 #1 hour
delta_per_period = 60 # 1 minute
timestamp_start = 1617291702
N = 10
wallet = "0x0822f3c03dcc24d200aff33493dc08d0e1f274a2"
res = getFirstWallets(number_of_periods, delta_per_period, timestamp_start, N)

for i in res:
    print(i['address'])
    for j in i['balances']:
        print(f'    {j}')



