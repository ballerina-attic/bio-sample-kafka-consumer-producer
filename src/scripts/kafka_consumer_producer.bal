import ballerinax/kafka;

configurable string groupId = "order-consumers";
configurable string orders = "orders";
configurable string paymentSuccessOrders = "payment-success-orders";
configurable decimal pollingInterval = 1;
configurable string kafkaEndpoint = kafka:DEFAULT_URL;

public type Order record {|
    int id;
    string desc;
    PaymentStatus paymentStatus;
|};

public enum PaymentStatus {
    SUCCESS,
    FAIL
}

public type OrderConsumerRecord record {|
    *kafka:AnydataConsumerRecord;
    Order value;
|};

final kafka:ConsumerConfiguration consumerConfigs = {
    groupId: groupId,
    topics: [orders],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval
};

service on new kafka:Listener(kafkaEndpoint, consumerConfigs) {
    private final kafka:Producer orderProducer;

    function init() returns error? {
        self.orderProducer = check new (kafkaEndpoint);
    }

    remote function onConsumerRecord(OrderConsumerRecord[] records) returns error? {
        check from OrderConsumerRecord {value} in records
            where value.paymentStatus == SUCCESS
            do {
                check self.orderProducer->send({
                    topic: paymentSuccessOrders,
                    value
                });
            };
    }
}
