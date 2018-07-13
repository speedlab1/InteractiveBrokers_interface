import ibapi
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from time import sleep
import threading
from datetime import datetime
from multiprocessing import Queue
from multiprocessing import queues
from ibapi.commission_report import CommissionReport
from ibapi.wrapper import TickerId
import pandas as pd


contract_dict = {"EURUSD" :("EUR",'USD'), 'EURGBP': ('EUR','GBP'), 'EURJPY': ('EUR','JPY'),'EURCHF':('EUR','CHF'), "EURCAD": ("EUR","CAD"), 'EURAUD': ('EUR','AUD'),'EURNZD':('EUR','NZD'),
                 "GBPUSD": ("GBP",'USD'),'USDJPY' :('USD','JPY'),'USDCHF':('USD','CHF'),'USDCAD':('USD','CAD'),'AUDUSD':('AUD','USD'),'NZDUSD':('NZD','USD'),
                 'GBPJPY' : ('GBP','JPY'),'GBPCHF':('GBP','CHF'),'GBPCAD':('GBP','CAD'),'GBPAUD':('GBP','AUD'),'GBPNZD':('GBP','NZD'),
                 "CHFJPY": ("CHF",'JPY'),'CADJPY':('CAD','JPY'),'AUDJPY':('AUD','JPY'), 'NZDJPY':('NZD','JPY'),
                 'CADCHF':('CAD','CHF'), 'AUDCHF':( 'AUD','CHF'),'NZDCHF':('NZD','CHF'),
                 'AUDCAD':('AUD','CAD'), 'AUDNZD':('AUD','NZD'),
                 'NZDCAD':('NZD','CAD'),
                 'USDSEK': ('USD','SEK'), 'USDRUB': ('USD','RUB'), 'USDNOK': ('USD','NOK'), 'USDMXN': ('USD','MXN'), 'USDDKK': ('USD','DKK'), 'USDCNH': ('USD','CNH'),
                 }
futures_dict = {'ZF':('ZF','USD','ECBOT','20171229','1000'),'ZB':('ZB','USD','ECBOT','20171219','1000'),'ZN':('ZN','USD','ECBOT','20171219','1000'),
           'BTP': ('BTP', 'EUR', 'DTB', '20171207', '1000'),'GBL':('GBL','EUR','DTB','20171207','1000'),'GBM':('GBM','EUR','DTB','20171207','1000'),
           'OAT': ('OAT', 'EUR', 'DTB', '20171207', '1000')}

KEYS = ['time','open','high','low','close']



class tickThread(threading.Thread):
    req_id = 0

    def __init__(self, threadID, conn):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.conn = conn

    def run(self):
        self.conn.run()

class IBInterface(EWrapper, EClient):
    wrapper_dict = {EWrapper.nextValidId.__name__: Queue(), EWrapper.openOrder.__name__: Queue(),
                    EWrapper.orderStatus.__name__: Queue(), EWrapper.updatePortfolio.__name__: Queue(),
                    EWrapper.execDetails.__name__: Queue(), EWrapper.commissionReport.__name__:Queue()}
    def __init__(self, host, port, client_id,account):
        self.host = host
        self.port = port
        self.open_orders = {}
        self.open_positions = {}
        self.req_id = 0
        self.account = account
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.connect(host, port, client_id)
        self.startApi()
        tickThread(101010, self).start()
        if self.isConnected():
            1
        else:
            while self.isConnected() == False:

                print('Sleeping 40 seconds')
                sleep(40)
                self.disconnect()
                self.done = False
                self.connect(host, port, client_id)
                self.startApi()
                tickThread(101010, self).start()
        self.reqAccountUpdates(True,account)


    #*********** Wrapper Functions *********************#
    def error(self, reqId:TickerId, errorCode:int, errorString:str):

        print('############## ' + str(reqId) + ' : ' + str(errorCode) + ' : ' + str(errorString) + ' ###################')
        if errorCode == 2107:
            contract = self.create_contract("EURJPY", 'FX')
            self.getHistoricalData(contract, '', '1 D', '30 mins', 'ASK', 1, 1)
    def nextValidId(self, orderId: int):

        while self.wrapper_dict[EWrapper.nextValidId.__name__].qsize()>0:
            self.wrapper_dict[EWrapper.nextValidId.__name__].get()
        self.wrapper_dict[EWrapper.nextValidId.__name__].put(orderId)
        print('Next valid ID ' + str(orderId))

    def realtimeBar(self, reqId: int, time: int, open: float, high: float, low: float, close: float, volume: int,
                    WAP: float, count: int):
        data_dict = {}
        data_dict['open'] = open
        data_dict['high'] = high
        data_dict['low'] = low
        data_dict['close'] = close
        data_dict['time'] = time
        data_dict['volume'] = volume
        self.wrapper_dict[EWrapper.realtimeBar.__name__+str(reqId)].put(data_dict)

    def connectionClosed(self):
        super().connectionClosed()
        print('Connection Closed')

    def historicalData(self, reqId:int , bars : object):

        data_dict = {}
        data_dict['open'] = bars.open
        data_dict['high'] = bars.high
        data_dict['low'] = bars.low
        data_dict['close'] = bars.close
        data_dict['time'] = bars.date
        data_dict['volume'] = bars.volume
        self.wrapper_dict[EWrapper.historicalData.__name__+str(reqId)].put(data_dict)

    def historicalDataEnd(self, reqId:int, start:str, end:str):
        super().historicalDataEnd(reqId,start,end)
        print('Historicla Data End')
        self.wrapper_dict[EWrapper.historicalData.__name__ + str(reqId)].put('End')

    # def historicalDataUpdate(self, reqId: int, bar: object):
    #     wrapper_dict[EWrapper.historicalDataUpdate.__name__ + str(reqId)].put(bar)

    def openOrder(self, orderId:int, contract:Contract, order:Order,
                  orderState:object):
        data_dict ={}
        data_dict['orderId'] = orderId
        print('Order open with orderID : '+str(orderId) + " for contract : "+str(contract.symbol)+str(contract.currency))
        data_dict['contract'] = contract
        data_dict['order'] = order
        data_dict['orderState']  =orderState
        self.wrapper_dict[EWrapper.openOrder.__name__].put(data_dict)

    def orderStatus(self, orderId:int , status:str, filled:float,
                    remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int,
                    whyHeld:str):
        data_dict = {}
        data_dict['status'] = status
        data_dict['filled'] = filled
        data_dict['remaining'] = remaining
        data_dict['avgFillPrice'] = avgFillPrice
        data_dict['permId'] = permId
        data_dict['parentId'] = parentId
        data_dict['lastFillPrice'] = lastFillPrice
        data_dict['clientId'] = clientId
        data_dict['whyHeld'] = whyHeld
        self.wrapper_dict[EWrapper.orderStatus.__name__].put(data_dict)

    def updatePortfolio(self, contract:Contract, position:float,
                        marketPrice:float, marketValue:float,
                        averageCost:float, unrealizedPNL:float,
                        realizedPNL:float, accountName:str):
        data_dict = {}
        data_dict['contract'] = contract
        data_dict['position'] = position
        data_dict['marketPrice'] = marketPrice
        data_dict['marketValue'] = marketValue
        data_dict['averageCost'] = averageCost
        data_dict['unrealizedPNL'] = unrealizedPNL
        data_dict['realizedPNL'] = realizedPNL
        data_dict['accountName'] = accountName
        self.wrapper_dict[EWrapper.updatePortfolio.__name__].put(data_dict)


    def execDetails(self, reqId:int, contract:Contract, execution:object):
        data_dict={}
        data_dict['contract'] = contract
        data_dict['execution'] = execution
        print("To API mas dinei auta " + str(execution.side) + ' '+contract.symbol+contract.currency + ' stis  '+str(execution.time) + " me timh "+str(execution.price) + ' kai lot '+str(execution.shares) + ' ston account '+ str(execution.acctNumber))
        self.wrapper_dict[EWrapper.execDetails.__name__].put(data_dict)

    def commissionReport(self, commissionReport:CommissionReport):
        data_dict = {}
        data_dict['commission'] = commissionReport
        print(commissionReport.realizedPNL)
        self.wrapper_dict[EWrapper.commissionReport.__name__].put(data_dict)


    #**************** Client Methods ******************#



    def req_next_valid_req_id(self):
        self.req_id += 1
        return self.req_id

    def check_conn(self):
        if (self.isConnected() == False):
            while self.isConnected() == False:
                sleep(60)
                self.connect(self.host, self.port, self.client_id)
        return True

    def check_req_id(self):
        self.nextValidId()

    def reqIds(self):
        super().reqIds(-1)
        sleep(0.5)
        queue = self.wrapper_dict[EWrapper.nextValidId.__name__]
        print("Testing ReqID")
        try:
            if queue.qsize()>0:
                id = queue.get(block=True,timeout=5)
            else:
                id =0
        except queues.Empty:
            id = 0
        return id

    def reqRealTimeBars(self, contract, barSize,
                        whatToShow, useRTH):
        req_id = self.req_next_valid_req_id()
        super().reqRealTimeBars(reqId=req_id, contract=contract, barSize=barSize,
                                whatToShow=whatToShow, useRTH=useRTH, realTimeBarsOptions=[])
        queue = Queue()
        self.wrapper_dict[EWrapper.realtimeBar.__name__+str(req_id)] = queue

        return queue




    def getHistoricalData(self,contract,end_date_time,duration_string,bar_size_setting, what_to_show,userth=1,format_date=2,keep_up_to_date = False ):
        req_id = self.req_next_valid_req_id()
        queue = Queue()
        self.wrapper_dict[EWrapper.historicalData.__name__ + str(req_id)] = queue
        super().reqHistoricalData(reqId=req_id,contract= contract,endDateTime=end_date_time,durationStr=duration_string,barSizeSetting=bar_size_setting,whatToShow=what_to_show,useRTH=userth,formatDate=format_date,keepUpToDate=keep_up_to_date,chartOptions=[])
        data = []
        queue = self.wrapper_dict[EWrapper.historicalData.__name__+str(req_id)]
        while True:
            try:
                z = queue.get(block=True,timeout=100)
                if z != 'End':
                    data.append(z)
                else:
                    break

            except queues.Empty:
                break


        return data


    def getOpenOrders(self):

        queue = self.wrapper_dict[EWrapper.openOrder.__name__]
        data = []
        print('At open Orders, queue size : '+str(queue.qsize()))
        try:

            for i in range(queue.qsize()):
            #TODO: Sometimes this will fail with queue.Empty
                data.append(queue.get(block=False))
            orders = self.set_open_orders(data)
        except queues.Empty:
            orders = []
            print("Queue Empty")

        return orders
    def getRealTimeHistoricalData(self,contract,end_date_time,duration_string,bar_size_setting, what_to_show,userth,format_date,keep_up_to_date = True):
        req_id = self.req_next_valid_req_id()
        queue = Queue()
        self.wrapper_dict[EWrapper.historicalDataUpdate.__name__ + str(req_id)] = queue
        queue2 = Queue()
        self.wrapper_dict[EWrapper.historicalData.__name__ + str(req_id)] = queue2
        super().reqHistoricalData(reqId=req_id, contract=contract, endDateTime=end_date_time,
                                  durationStr=duration_string, barSizeSetting=bar_size_setting, whatToShow=what_to_show,
                                  useRTH=userth, formatDate=format_date,keepUpToDate=keep_up_to_date,chartOptions=[])
        queue2 = self.wrapper_dict[EWrapper.historicalData.__name__ + str(req_id)]
        data = []
        while True:
            try:
                z = queue2.get(block=True)
                if z != 'End':
                    data.append(z)
                else:
                    break
            except queues.Empty:
                break

        return queue,data

    def getAccountPositions(self,account):
        super().reqAccountUpdates(True,account)
        queue = self.wrapper_dict[EWrapper.updatePortfolio.__name__]

        data = []
        try:

            for i in range(queue.qsize()):
                data.append(queue.get(block=False))
            positions = self.set_open_positions(data)
        except queues.Empty:
            positions = {}
            print("Queue Empty")




        return positions


    def currentTime(self, time: int):
        super().currentTime(time)
        self.serverTime = time
        print(time)

    def create_contract(self,symbol, contract_type):
        if contract_type == "FX":
            contract = Contract()
            contract.symbol = contract_dict[symbol][0] #FOAT DEC 17
            contract.secType = "CASH" #FUT
            contract.currency = contract_dict[symbol][1] #EUR
            contract.exchange = "IDEALPRO" #DTP
            return contract
        elif contract_type=='FUTURE':
            contract = Contract()
            contract.symbol = futures_dict[symbol][0]
            contract.secType = "FUT"
            contract.currency = futures_dict[symbol][1]
            contract.exchange = futures_dict[symbol][2]
            contract.lastTradeDateOrContractMonth=futures_dict[symbol][3]
            contract.multiplier = futures_dict[symbol][4]
            return contract

    def create_order(self,action,quantity,type,price = 0):
        order = Order()
        order.orderType = type
        if type != 'MKT':
            order.lmtPrice = price
        order.action = action
        order.totalQuantity = quantity
        order.account = self.account
        print('Parent Order Quantity '+str(quantity))
        return order

    def sell(self,quantity,contract,open_position,take_profit_price=0,stop_loss_price=0,limit_orders_lot=0,is_combined=False,reduce_lot = False):
        total_orders = []
        parent_id = self.reqIds()
        sleep(0.7)
        print('Apo to sell to ID einai : '+str(parent_id))
        other_id = parent_id

        order= self.create_order('SELL',quantity+abs(open_position),'MKT')
        order.orderId = parent_id
        total_orders.append(order)
        if take_profit_price!= 0:

            take_profit_order = Order()

            other_id +=1
            take_profit_order.orderId = other_id
            if reduce_lot:
                take_profit_order.action = 'SELL'
            else:
                take_profit_order.action = 'BUY'
            take_profit_order.lmtPrice = take_profit_price
            take_profit_order.orderType = 'LMT'
            take_profit_order.totalQuantity = limit_orders_lot
            print('Take Profit Order Quantity :'+str(take_profit_order.totalQuantity))
            take_profit_order.tif = 'GTC'
            take_profit_order.ocaType = 1
            take_profit_order.account = self.account
            take_profit_order.ocaGroup = contract.symbol+contract.currency + str(parent_id)
            total_orders.append(take_profit_order)
        if stop_loss_price !=0:

            stop_loss_order = Order()

            other_id += 1
            stop_loss_order.orderId = other_id
            if reduce_lot:
                stop_loss_order.action = 'SELL'
            else:
                stop_loss_order.action = 'BUY'
            stop_loss_order.auxPrice = stop_loss_price
            stop_loss_order.orderType = 'STP'
            stop_loss_order.totalQuantity = limit_orders_lot
            print('Stop Loss Order Quantity :' + str(stop_loss_order.totalQuantity))
            stop_loss_order.tif = 'GTC'
            stop_loss_order.ocaType = 1
            stop_loss_order.account = self.account
            stop_loss_order.ocaGroup = contract.symbol+contract.currency + str(parent_id)
            total_orders.append(stop_loss_order)

        orders = self.getOpenOrders()
        if is_combined==False:
            for open_order in orders:
                if contract.symbol + contract.currency in orders[open_order][1].ocaGroup:
                    self.cancelOrder(open_order)
        for every_order in total_orders:
            self.placeOrder(every_order.orderId, contract,every_order)
            sleep(2)

        return self.wrapper_dict[EWrapper.openOrder.__name__], self.wrapper_dict[EWrapper.orderStatus.__name__]

    def buy(self,quantity,contract,open_position,take_profit_price=0,stop_loss_price=0,limit_orders_lot=0, is_combined=False,reduce_lot = False):
        total_orders = []
        parent_id = self.reqIds()
        sleep(0.7)
        print('Apo to buy to ID einai : ' + str(parent_id))
        other_id = parent_id
        order= self.create_order('BUY',quantity+abs(open_position),'MKT')
        order.orderId = parent_id
        total_orders.append(order)
        if take_profit_price!= 0:

            take_profit_order = Order()

            other_id +=1
            take_profit_order.orderId = other_id
            if reduce_lot:
                take_profit_order.action = 'BUY'
            else:
                take_profit_order.action = 'SELL'
            take_profit_order.lmtPrice = take_profit_price
            take_profit_order.orderType = 'LMT'
            take_profit_order.totalQuantity = limit_orders_lot
            take_profit_order.tif = 'GTC'
            take_profit_order.ocaType = 1
            take_profit_order.account = self.account
            take_profit_order.ocaGroup = contract.symbol+contract.currency  + str(parent_id)
            total_orders.append(take_profit_order)
        if stop_loss_price !=0:

            stop_loss_order = Order()

            other_id += 1
            stop_loss_order.orderId = other_id
            if reduce_lot:
                stop_loss_order.action = 'BUY'
            else:
                stop_loss_order.action = 'SELL'
            stop_loss_order.auxPrice = stop_loss_price
            stop_loss_order.orderType = 'STP'
            stop_loss_order.totalQuantity = limit_orders_lot
            stop_loss_order.tif = 'GTC'
            stop_loss_order.ocaType = 1
            stop_loss_order.account = self.account
            stop_loss_order.ocaGroup = contract.symbol+contract.currency + str(parent_id)
            total_orders.append(stop_loss_order)
        orders = self.getOpenOrders()
        if is_combined == False:
            for open_order in orders:
                if contract.symbol+contract.currency in orders[open_order][1].ocaGroup:
                    self.cancelOrder(open_order)
        for every_order in total_orders:
            self.placeOrder(every_order.orderId, contract,every_order)


        return self.wrapper_dict[EWrapper.openOrder.__name__], self.wrapper_dict[EWrapper.orderStatus.__name__]

    def set_open_positions(self,queue_positions):
        for order in queue_positions:
            self.open_positions[order['contract'].symbol+ order['contract'].currency] = order['position']

        return self.open_positions

    def set_open_orders(self,queue_orders):
        for order in queue_orders:
            self.open_orders[order['orderId']] = (order['contract'],order['order'],order['orderState'])

        return self.open_orders

    def reqExecutionDetails(self,filter):

        self.reqExecutions(reqId=1,execFilter=filter)
        queue =  self.wrapper_dict[EWrapper.execDetails.__name__]
        queue_commision = self.wrapper_dict[EWrapper.commissionReport.__name__]
        return queue, queue_commision

    def getAccountPositionsQueue(self,account):
        super().reqAccountUpdates(True,account)
        queue = self.wrapper_dict[EWrapper.updatePortfolio.__name__]
        return queue

def store_historical_data_to_file(symbol, currency, data):
    data = pd.DataFrame(data)
    data = data.set_index(pd.to_datetime(data['time']))
    data = data.drop('time',axis =1)

    store = pd.HDFStore('tick_data' + symbol + currency + '.h5')
    store.open()

    if ('/'+symbol+currency) in store.keys():
        idx = store.select(symbol+currency,where="index in data.index",columns=['index']).index

        print('Store ' + symbol + currency + ' was written with length ' + str(len(data.query('index not in @idx'))))
        # print(dataframe)
        store.append(symbol + currency, data.query('index not in @idx'), format='t')
    else:
        store.append(symbol + currency, data, format='t')
        print(data)
    store.close()

# def store_queue_to_file(queue,symbol,currency):
#
#             msg_array = []
#             times = []
#             while True:
#                 try:
#                     t1 = time.time()
#                     now = datetime.now()
#                     msg = queue.get(block = True)
#                     msg_array.append(msg)
#                     times.append(time.time() - t1)
#                     if pd.to_datetime(msg['time'], unit='s').second == 55:
#                         break
#
#
#                 except queues.Empty:
#                     break
#             data_dict_array = []
#             if len(msg_array) > 1:
#                 for msg in msg_array:
#                     data_dict = {}
#                     for key in KEYS:
#                         data_dict[key] = msg[key]
#                     data_dict_array.append(data_dict)
#
#                 dataframe = pd.DataFrame(data=data_dict_array)
#                 dataframe['time'] = pd.to_datetime(dataframe['time'], unit='s')
#
#
#                 dataframe = dataframe.set_index(pd.to_datetime(dataframe['time']))
#                 dataframe = dataframe.drop('time', axis=1)
#                 print(dataframe.index)
#
#                 store = pd.HDFStore('tick_data' + symbol + currency + '.h5')
#                 store.open()
#                 print('Store ' + symbol + currency + ' was written with length ' + str(len(dataframe)))
#                 # print(dataframe)
#                 store.append(symbol + currency, dataframe, format='t')
#
#                 store.close()

# def set_open_orders(orders):
#     for order in orders:
#
#         if order['order'].ocaGroup != '':
#             open_orders[order['orderId']] = order['order'].ocaGroup


# if __name__ == '__main__':
#
#     test = IBInterface(host='127.0.0.1', port=7496, client_id=101011,account = 'DU517095')
#     test.startApi()
#     #test.reqAllOpenOrders()
#
#
#     import time
#
#     time.sleep(1)
#
#     tickThread(101010, test).start()
#     test.reqIds()
#     test.reqCurrentTime()
#     print("Order ID " + str(test.nextValidOrderId))
#     sleep(2)
#     orders = test.getOpenOrders()
#     open_positions = test.getAccountPositions('DU517095')
#     #set_open_orders(orders)
#     contract = test.create_contract("EURJPY", 'FX')
#
#     test.sell(10000,contract,open_positions['EURJPY'],127.5,128.4)
#     x = test.reqIds()
#     x = test.check_conn()
#     #queryTime = datetime.now().replace(month = -6).strftime("yyyyMMdd HH:mm:ss")
#     #y, data = test.getRealTimeHistoricalData(contract,'','1 D','30 secs','MIDPOINT',1,1)
#     real_time_bars = {}
#     # for contracts in contract_dict:
#     #     contract = test.create_contract(contracts, 'FX')
#     #
#     #     data = test.getHistoricalData(contract,'','2 D','1 min','ASK',1,1)
#     #     store_historical_data_to_file(contract_dict[contracts][0], contract_dict[contracts][1],data)
#     #
#     # for contracts in contract_dict:
#     #     contract = test.create_contract(contracts, 'FX')
#     #     real_time_bars[contracts] = test.reqRealTimeBars(contract, 5, 'ASK', True)
#     # sleep(2)
#     # 1
#     # # contract = test.create_contract('AUDCAD','FX')
#     # #
#     # # y = test.reqRealTimeBars(contract, 5, "ASK", True)
#     # # print(x)
#     # while True:
#     #     for contracts in contract_dict:
#     #         if test.isConnected():
#     #
#     #             store_queue_to_file(real_time_bars[contracts], contract_dict[contracts][0], contract_dict[contracts][1])