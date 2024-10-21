import re
import werkzeug
from pymongo import MongoClient
from flask import (Flask, request, jsonify, abort)

def create_app():
    app = Flask(__name__)
    mongoClient = MongoClient(host='localhost', port=27017)

    db = mongoClient.flaskDatabase
    clientsCollection = db.clients
    productsCollection = db.products
    ordersCollection = db.orders

    orderCounter = 0

    @app.route('/clients', methods=['PUT'])
    def registerClient():
        data = request.get_json()

        if not data or 'name' not in data or 'email' not in data:
            return 'Invalid input, missing name or email', 400
        
        existingClient = clientsCollection.find_one({'_id': data['id']})
        if existingClient:
            return 'Client with this id already exists', 400

        newClient = {
            '_id': data['id'],        
            'name': data['name'],
            'email': data['email']
        }

        clientsCollection.insert_one(newClient)

        clientCounter += 1

        return jsonify({'id': newClient['_id']}), 201

    @app.route('/clients/<clientId>', methods=['GET'])
    def getClient(clientId):
        client = clientsCollection.find_one({'_id': clientId})

        if not client:
            return 'Client not found', 404

        clientData = {
            'id': client['_id'],
            'name': client['name'],
            'email': client['email']
        }

        return jsonify(clientData), 200

    @app.route('/clients/<clientId>', methods=['DELETE'])
    def deleteClient(clientId):
        client = clientsCollection.delete_one({'_id': clientId})
        
        if client.deleted_count == 0:
            return 'Client not found', 404

        ordersCollection.delete_many({'clientId': clientId})

        return 'Client deleted', 204

    @app.route('/products', methods=['PUT'])
    def registerProduct():
        data = request.get_json()

        if not data or 'id' not in data or 'name' not in data or 'price' not in data or data['price']<0:
            return 'Invalid input, missing name or price', 400

        existingProduct = productsCollection.find_one({'_id': data['id']})
        if existingProduct:
            return 'Product with this id already exists', 400

        newProduct = {
            '_id': data['id'],
            'name': data['name'],
            'category': data['category'],
            'description': data['description'],
            'price': data['price']
        }
        productsCollection.insert_one(newProduct)

        return jsonify({'id': newProduct['_id']}), 201

    @app.route('/products', methods=['GET'])
    def listProducts():
        try:
            data = request.get_json()
        except Exception as e:
            data = None

        if not data or 'category' not in data:
            productList = list(productsCollection.find({}))
            return jsonify(productList), 200

        productList = list(productsCollection.find({'category': data['category']}))
        return jsonify(productList), 200

    @app.route('/products/<productId>', methods=['GET'])
    def getProductDetails(productId):
        product = productsCollection.find_one({'_id': productId})

        if not product:
            return 'Product not found', 404

        return jsonify(product), 200

    @app.route('/products/<productId>', methods=['DELETE'])
    def deleteProduct(productId):
        product = productsCollection.find_one({'_id': productId})

        if not product:
            return 'Product not found', 404

        productsCollection.delete_one({'_id': productId})

        return 'Product deleted', 204

    @app.route('/orders', methods=['PUT'])
    def createOrder():
        global orderCounter
        orderCounter = ordersCollection.count_documents({})

        data = request.get_json()

        if not data or 'items' not in data or 'clientId' not in data:
            return 'Invalid input, missing clientId or items', 400

        if not clientsCollection.find_one({'_id': data['clientId']}):
            return 'Client not found', 404

        for product in data['items']:
            if not productsCollection.find_one({'_id': product['productId']}):
                return 'Product not found', 404
            
            if product['quantity'] < 1:
                return 'Invalid quantity', 400

        newOrder = {
            '_id': f"ord{orderCounter+1}",
            'clientId': data['clientId'],
            'items': data['items']
        }
        orderCounter += 1
        ordersCollection.insert_one(newOrder)

        return jsonify({'id': newOrder['_id']}), 200

    @app.route('/clients/<clientId>/orders', methods=['GET'])
    def getClientOrders(clientId):
        if not clientsCollection.find_one({'_id': clientId}):
            return 'Client not found', 404

        orderList = list(ordersCollection.find({'clientId': clientId}))
        for order in orderList:
            order.pop('clientId', None)

        return jsonify(orderList), 200

    @app.route('/statistics/top/clients', methods=['GET'])
    def getTopTenClients():
        clientList = list(ordersCollection.aggregate([
            {
                "$group": {
                    "_id": "$clientId",  
                    "totalOrders": { "$sum": 1 }  
                }
            },
            { 
                "$sort": { "totalOrders": -1 }  
            },
            { 
                "$limit": 10  
            },
            {
                "$project": {
                    "_id": 0,  
                    "clientId": "$_id",  
                    "totalOrders": 1  
                }
            }
        ]))


        return jsonify(clientList), 200

    @app.route('/statistics/top/products', methods=['GET'])
    def getTopTenProducts():
        productList = list(ordersCollection.aggregate([
            {
                "$unwind": "$items" 
            },
            {
                "$group": {
                    "_id": "$items.productId", 
                    "totalQuantity": { "$sum": "$items.quantity" }
                }
            },
            {
                "$sort": { "totalQuantity": -1 } 
            },
            {
                "$limit": 10 
            },
            {
                "$project": {
                    "_id": 0,  
                    "productId": "$_id",  
                    "totalQuantity": 1 
                }
            }
        ]))

        return jsonify(productList), 200

    @app.route('/statistics/orders/total', methods=['GET'])
    def getTotalOrdersNumber():
        return jsonify({'total': ordersCollection.count_documents({})}), 200

    @app.route('/statistics/orders/totalValue', methods=['GET'])
    def getTotalValueOfOrders():
        totalValue = list(ordersCollection.aggregate([
            {
                "$unwind": "$items"  
            },
            {
                "$lookup": { 
                    "from": "products",
                    "localField": "items.productId",
                    "foreignField": "_id",
                    "as": "productDetails"
                }
            },
            {
                "$unwind": "$productDetails"
            },
            {
                "$group": {
                    "_id": None,
                    "totalValue": {
                        "$sum": { "$multiply": ["$items.quantity", "$productDetails.price"] }
                    }  
                }
            },
            {
                "$project": {
                    "_id": 0,  
                    "totalValue": 1 
                }
            }
        ]))

        return jsonify(totalValue[0]), 200

    @app.route('/cleanup', methods=['POST'])
    def deleteAllData():
        for collection in db.list_collection_names():
            db[collection].delete_many({})

        return 'Data deleted', 204

    return app

