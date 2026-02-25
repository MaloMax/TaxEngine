import sys, requests, pandas as pd, time, os, pickle, sqlite3
sys.path.append(os.path.dirname(os.getcwd()))  # Per lib
from crypto_tax_lib import tax_lib

#https://mempool.space/api/address/1HQWgexC7u35a6m3cxBNRwqQ7vkwdnA1HE/txs/chain

keys = {
#nuovo1
'bc1q0dcje9wtpglg83dcca0a0y4uuc0f3lyafcn62s': 2, 'bc1q7qgxa39mfrlr8l7jrurm8ftpm0sm4zym3m084r': 1, 'bc1qxx0sk2arcw5lx06x9xfjuh89ccdvwvk9e33q8w': 1, 'bc1qk4632hywer3cjgmezwxrhazestyzwpspxeahd3': 1, 'bc1qtqntg36nkts38gx7xc7y23c6r6sly6zyjww8my': 1, 'bc1q2rm5mhzdl20ade659r4jzjtpxh4m0jsmcyme3w': 1,

#nuovo2

'bc1qmkwhfsk0mgvsfmld4e2vvvhuunh7hzh68tfrxs': 2, 'bc1qjjlplw87hjxuzzt2amtrp40gssy0lj7pe9w6um': 2, 'bc1qakwlr2qjvn4tu5quyse48d3ufd4ynmudwdam26': 2, 'bc1q8n9r658a00p3k5pz5tmgvdn8ev687dn7c96m36': 2, 'bc1qscmckfcaduu9rwcxsxquurs058jgfgryy6aczy': 2, 'bc1qqdxhw52a7njz8vpad7ch9haflt62pvs08922qp': 4, 'bc1qa0hcxgu4ejk0j6audqerhuyzq382p7ke5a60r5': 2, 'bc1qv27t2x35v3d4wll3z4pcy00m77qude4wh4kqc3': 2, 'bc1qypqcwd2fvju0pz2qmf7jc9990lrp4yk57euvza': 1, 'bc1q8tr3v7epsge47yqe7ywa0ttxakze47njph50wt': 3, 'bc1qyw5s9nna0f9kcw9a0z63f4jzt0vr5nc5e4gxec': 2, 'bc1q4hv8l4g5j4pzanwc5yuhfssw8utz4pvzypga0h': 2, 'bc1qk0anetvyswuwn30898krzshkd862ytn744pug9': 2, 'bc1qde74h77v7527xvzgkupu3q4qajzz6cpx39cysf': 2, 'bc1qs99c4lntjtc6qe83vx47nnfytz29f0x72l008u': 2, 'bc1qap29ycz63dsu4klkh397udav4u2k7zwkglg2es': 2, 'bc1q35lvgww0yurtw98hjwqgzhzzvrggk4lym6fyj9': 2, 'bc1qkaqwhd3gjj30xfxs2khcgldtuvzlvfl50d7yds': 2, 'bc1qwxjecx2kyefxmlmn8fxcegg5l49tpu6dej9anh': 2, 'bc1qzm5nsncwuqvxxf9gndr3gag77vhctpku8kpcac': 2, 'bc1qrshsr6x6dnjr042nz0z6ttnpxp7dcdxp4tj5r4': 2, 'bc1q76l0mufqp3864f380036jlnmx65gc4nalg0mwy': 2, 'bc1q5jg5tvy4wef4uj9zuad87l2pl043k5dvwua6vd': 2, 'bc1qkmg3n96p0fc0787vgqe9cpp2dp4gxvrgvtz5gv': 2, 'bc1q5mydhdedq2eaahtzwfgw659m5vlv2g53a4j4z5': 1, 'bc1q5rsznfg3lvyv2ca9lmh4fatz589tamnnj9lcxv': 1,


#Antico
'1EnZ1XLyF4fdw6u3at3CDTczbr4whH1siw': 2, '1EaRTktYtBy4MCUHZf5Mazjj7VPPaEuKBk': 5, '16FQWgBRGbg8JnX3rRPxSfy4k8oCfCBhyQ': 2, '1KCA9uHbxgazEAwZij9WU6tzPphVsPQoF1': 4, '1FuSFGfxe53KNiPemcaBWhddNvNq2nxpan': 2, '1AqvtQfVXtNw44Jshs4nmty8ByR3q4Uis1': 2, '12U6mtUKsBdqx9PxLBrNS4ajgsJJFcEMr4': 2, '1HUzgTzyZSgbvspHP1xx58hCrj6KPXNwtj': 2, '158kxRdraE4yqBPVt5A6Ybw8xNxypWphbd': 2, '16yAQbC12MspFHN8kqvQrMBi3wPoqSAh56': 2, '1CXrREtFy9z6LppYBjwUamfHwYBdwnxD26': 3, '1N3KGpqTC1r1PDRe1sxQZofLrnzBMz3RCB': 2, '1bhsBpxqxXrbuXyXmeg34sUw9YywFeVJb': 2, '1zAitkTvph4np2AbfsRZTCzBD2MkiqdJo': 2, '1ETjoycrF3FxjxbnVBfKYQVXs7Estnw8qp': 2, '1LbkBWrnkUiNic49NhrPqi68mD78VspAei': 2, '1AUL6og6bCBxgtHfFGUHNrL4SNRqwQHff4': 2, '17nFAyQnK28gFiLVGU7VKcnycgmzf7CVyr': 2, '129gvLknQXcBpnerHQJLmoTRrJxtqtkLyU': 2, '1HQWgexC7u35a6m3cxBNRwqQ7vkwdnA1HE': 2,

#SegWit1 0
'bc1qxkfe3z2pnew5uq0rfw7g705al3v068u79ez6sv': 2, 'bc1qe5jln97p8vhupk9g0quh58zfxn35ygnh2mmn57': 2, 'bc1qkjajl7xnr74nradyewsg025hnhdp5pnft3nlru': 2, 'bc1q6xe6wp2gqedazquv407gdej7rmx3wtmaqkphf7': 3, 'bc1qh6na7upyapysll60hsd7jmsy2lewdt6ycwmfwd': 4, 'bc1q6aeuh895ay6etqgkwcvy9rcczqqeenjfssnn7z': 2, 'bc1qcya0j90k6sglqz92fc25aj7x0hj7tdmtrwgw3l': 2, 'bc1qdxvw2sg96h33u6t2vgtmlhyhppvcy7f37au8yh': 2, 'bc1qsh4pf07z8vwlkv2mvfdur7epc39kygcyp6df5x': 2, 'bc1q34qxyy0m9s0n6kpsteujeen5qskjs8kexxqh87': 2, 'bc1qqsxfr2qd7qrpr8nc2t7xaghyhdqh8zqkpnqll9': 2, 'bc1qec47t5nhunrxc6mvph3qc5pmjr8qmmpwmevk0x': 2,

#lightining
'bc1qrg0y9mtrcpc79ydvrle4w2ejs5zjrqn9haq7uj': 2, 'bc1qp6jkmuzqmph2jajqde692uzd65yeqp0vd7fp82': 2, 'bc1q8ma3pyzgh9rwgmjd0nl0vskxypg5wulwp5qw83': 3,

#nuovo T
'bc1q3c0f7k8e75t2mw06932fvwkuwy08e56gw0pk5v': 2, 'bc1qplphmpj7ltedt0au5dcflytvpadw96s8u4l5sw': 2,


'13j1zprAvYwYu7o3ahiaWW6koBF5DEK99g': 2, # KzPdD2uZzaJanB5y4K3oUSNBPAg1yMni6Effz4Y2hbqyxMkrYaps

'1AANaeGQYx6oYjK5uGjiGYMaT6nHR8chH3': 2, #  KyvSRAKPwz8y9n73dx1gJNXA9V4KBKruy9h5zqGvd73wGmSqTqWu

'1M39a7QanTGNTdqCnq5yZhvdovjxa1MCFu': 2} #  L2qsnC1p6A3GpSanVryTn4V2JAxXMFSFGJqL89MHjrDbQZWHb4oU


BASE_URL = "https://mempool.space/api"
BASE_DIR = "btc_data"
TX_DIR = os.path.join(BASE_DIR, "tx_raw")
ADDR_FILE = os.path.join(BASE_DIR, "addresses.pkl")

os.makedirs(TX_DIR, exist_ok=True)

def load_txs(address):
    path = os.path.join(TX_DIR, f"{address}.pkl")
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    return None

def save_txs(address, txs):
    path = os.path.join(TX_DIR, f"{address}.pkl")
    with open(path, "wb") as f:
        pickle.dump(txs, f)
    
def get_all_txs(address, sleep=0.2):

    
    all_txs = load_txs(address)
    
    if all_txs is None:
        all_txs = []
        
        print('Scarico ',address)
        last_txid = None
        while True:
            url = f"{BASE_URL}/address/{address}/txs/chain"
            if last_txid:
                url += f"/{last_txid}"

            r = requests.get(url, timeout=30)
            r.raise_for_status()
            txs = r.json()

            if not txs:
                break

            all_txs.extend(txs)
            last_txid = txs[-1]["txid"]

            if len(txs) < 25:
                break

            time.sleep(sleep)

        save_txs(address, all_txs)
        
    return all_txs



my_addresses = set(keys.keys())

print(len(my_addresses))
rows = []

for address in my_addresses:
    
    txs = get_all_txs(address)

    for tx in txs:
        txid = tx["txid"]
        status = tx["status"]
        ts = status["block_time"]
        fee = tx.get("fee", 0)

        # ---- INPUT: se sto spendendo
        spent = False
        for vin in tx["vin"]:
            prev = vin.get("prevout")
            if not prev:
                continue

            from_addr = prev["scriptpubkey_address"]

            if from_addr == address:
                spent = True
                break

        # ---- OUTPUT
        for vout in tx["vout"]:
            to_addr = vout.get("scriptpubkey_address")

            if to_addr == address:
                amount = vout["value"]
                # ENTRATA
                rows.append({
                    "txid": txid,
                    "timestamp": ts,
                    "my_address": address,
                    "other_address": from_addr,
                    "direction": "IN",
                    "amount_sat": amount,
                    "fee_sat": 0,
                    "internal": from_addr in my_addresses
                })

            elif spent:
                amount = prev["value"]
                # USCITA
                rows.append({
                    "txid": txid,
                    "timestamp": ts,
                    "my_address": address,
                    "other_address": to_addr,
                    "direction": "OUT",
                    "amount_sat": amount,
                    "fee_sat": fee,
                    "internal": to_addr in my_addresses
                })
df = pd.DataFrame(rows)

df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
df = df.sort_values("datetime")

df['signed_amount'] = 0

oldTxId = None
oldAddr = None

for idx, row in df.iterrows():
    
    
    direc = row.get('direction')
    TxId = row.get('txid')
    
    if direc == 'IN':
        df.at[idx, 'signed_amount'] = row.get('amount_sat', 0)
    else:
        Addr = row.get('other_address')
        if oldTxId != TxId:
            df.at[idx, 'signed_amount'] = -(int(row.get('amount_sat', 0)) + int(row.get('fee_sat', 0)))
            AddrList = [Addr]
        else :
            if Addr not in AddrList:
                df.at[idx, 'signed_amount'] = -(int(row.get('amount_sat', 0)))
                AddrList.append(Addr)
                
        oldTxId = TxId
    
    
    #address = row.get('other_address')
    
    

df.to_csv("AddressBC.csv", index=False)
'''

# INTEGRAZIONE DB
df['amount_btc'] = df['amount_sat'] / 100000000
df['timestamp'] = df['timestamp'].astype(int)
df['internal'] = df['internal'].astype(int)

for _, row in df.iterrows():
    if row['direction'] in ['IN', 'OUT']:
        pd.DataFrame([{
            'wallet_address': row['my_address'],
            'txid': row['txid'],
            'direction': row['direction'],
            'amount_btc': row['amount_btc'],
            'timestamp': row['timestamp'],
            'other_address': row['other_address'],
            'fee_sat': row['fee_sat'],
            'internal': row['internal']    
        }]).to_sql('wallet_bc', tax_lib.conn, if_exists='append', index=False)

print("✅ Wallet BC registrati!")
tax_lib.report_status()

# MATCH AUTOMATICO CEX→WALLET
tax_lib.match_wallet_in_cex()
'''