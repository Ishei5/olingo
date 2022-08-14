package org.apache.olingo.test;

import com.google.common.collect.Lists;
import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.request.retrieve.RetrieveRequestFactory;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.*;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.springframework.data.util.Pair;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class MyExample {

    private static final String ORDER_ENTRY_SET_NAME = "Document_ЗаказПокупателя";
    private static final String ORDER_SELECT_FIELDS = "Ref_Key,Number,Date,АдресДоставки,Контрагент/Description,Ответственный/Code";
    private static final String ORDER_EXPAND_FIELDS = "Ответственный/ФизЛицо,Контрагент";
    private static final String PRODUCT_ENTRY_SET_NAME = "Document_ЗаказПокупателя_Товары";
    private static final String PRODUCT_SELECT_FIELDS = "Ref_Key,Количество,КоличествоМест,Коэффициент,ЕдиницаИзмерения/Description,Номенклатура/Description";
    private static final String PRODUCT_EXPAND_FIELDS = "Номенклатура,ЕдиницаИзмерения";
    private static final LocalDate DATE = LocalDate.of(2020, 3, 5);
    private static final int PARTITION_SIZE = 100;

    public static final String URI = "http://89.76.239.245:8080/test/odata/standard.odata/";
    private final ODataClient client;

    public MyExample() {
        this.client = ODataClientFactory.getClient();
    }

    public static void main(String[] args) {
        MyExample myExample = new MyExample();
        double end = myExample.perform(URI);
        System.out.println("Finished - " + end);
    }

    private double perform(String uri) {
        long startTime = System.currentTimeMillis();
        URI orderUri = buildOrderUri(createDateFilter(DATE.atStartOfDay().toString()));
        ClientEntitySet clientEntitySet = readEntities(orderUri);
        List<Map<String, ClientValue>> clientMap = clientEntitySetToListOfMap(clientEntitySet);

        Map<String, OrderDto> orders = clientMap.stream()
                .map(this::parseOrder)
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (v1, v2) -> v1));


        List<List<String>> partition = getPartitionOfProductKeys(orders);

        /**
        * String with Ref_Keys -> Prepared URL -> ClientEntrySet -> List<Map<ClientPropertyName, ClintProperty>
        * Map<Ref_key, List<ProductDto> -> collect maps to single map
        **/
        Map<String, List<ProductDto>> allProducts = parseAllProducts(partition);

        enrichOrders(orders, allProducts);

        return ((System.currentTimeMillis() - startTime) / 1000);

    }

    private ClientEntitySet readEntities(URI absoluteUri) {
        System.out.println("URI = " + java.net.URLDecoder.decode(String.valueOf(absoluteUri), StandardCharsets.UTF_8));
        RetrieveRequestFactory retrieveRequestFactory = client.getRetrieveRequestFactory();
        ODataEntitySetRequest<ClientEntitySet> entitySetRequest = retrieveRequestFactory.getEntitySetRequest(absoluteUri);
        ODataRetrieveResponse<ClientEntitySet> response = entitySetRequest.execute();

        return response.getBody();
    }

    private List<Map<String, ClientValue>> clientEntitySetToListOfMap(ClientEntitySet clientEntitySet) {
        return clientEntitySet.getEntities().stream()
                .map(ClientEntity::getProperties)
                .map(clientProperties -> clientProperties.stream()
                        .collect(Collectors.toMap(ClientProperty::getName, ClientProperty::getValue)))
                .toList();
    }

    private Pair<String, OrderDto> parseOrder(Map<String, ClientValue> orderProperties) {
        String refKey = parsePrimitive(orderProperties.get("Ref_Key"));

        OrderDto order = OrderDto.builder()
                .orderNumber(parsePrimitive(orderProperties.get("Number")))
                .createdDate(parseDate(orderProperties.get("Date")))
                .address(parsePrimitive(orderProperties.get("АдресДоставки")))
                .clientName(parseComplex(orderProperties.get("Контрагент")))
                .managerFullName(parseComplex(orderProperties.get("Ответственный")).trim())
                .build();

        return Pair.of(refKey, order);
    }

    private void enrichOrders(Map<String, OrderDto> orders, Map<String, List<ProductDto>> allProducts) {
        for (Map.Entry<String, OrderDto> entry: orders.entrySet()) {
            String key = entry.getKey();
            OrderDto order = entry.getValue();
            List<ProductDto> products = allProducts.get(key);
            order.setProducts(products);
            order.setOrderWeight(getTotalOrderWeight(products));
        }
    }

    private List<List<String>> getPartitionOfProductKeys(Map<String, OrderDto> orders) {
        return Lists.partition(orders.keySet().stream().toList(), PARTITION_SIZE);
    }

    private Map<String, List<ProductDto>> parseAllProducts(List<List<String>> partition) {
        return partition.stream()
                .map(this::createProductsFilter)
                .map(this::buildProductUri)
                .map(this::readEntities)
                .map(this::clientEntitySetToListOfMap)
                .map(mapOfProductListFunction())
                .flatMap(map -> map.entrySet().stream())
                .collect(productMapCollector());
    }

    private Pair<String, ProductDto> parseProduct(Map<String, ClientValue> productProperties) {
        String key = parsePrimitive(productProperties.get("Ref_Key"));
        List<String> allowableMeasure = List.of("кг", "л", "шт");
        int amount = Integer.parseInt(parsePrimitive(productProperties.get("КоличествоМест")));
        String measure = parseComplex(productProperties.get("ЕдиницаИзмерения"));
        double totalWeight = allowableMeasure.contains(measure) ?
                (!measure.equals("шт")) ? Double.parseDouble(parsePrimitive(productProperties.get("Количество")).toString()) : 1.0
                : 0.0;
        String storeName = "Склад №1";

        ProductDto product = ProductDto.builder()
                .productName(parseComplex(productProperties.get("Номенклатура")))
                .amount(amount)
                .unitWeight(amount == 0 ? 0 : totalWeight / amount)
                .totalProductWeight(totalWeight)
                .storeName(storeName)
                .build();
        return Pair.of(key, product);
    }

    private double getTotalOrderWeight(List<ProductDto> products) {
        return products.stream()
                .mapToDouble(ProductDto::getTotalProductWeight)
                .sum();
    }

    private Collector<Map.Entry<String, List<ProductDto>>, ?, Map<String, List<ProductDto>>> productMapCollector() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1);
    }

    private Function<List<Map<String, ClientValue>>, Map<String, List<ProductDto>>> mapOfProductListFunction() {
        return product -> product.stream()
                .map(this::parseProduct)
                .collect(PairToMapCollector());
    }

    private Collector<Pair<String, ProductDto>, ?, Map<String, List<ProductDto>>> PairToMapCollector() {
        return Collectors.groupingBy(Pair::getFirst, Collectors.mapping(Pair::getSecond, Collectors.toList()));
    }

    /*URI*/
    URI buildOrderUri(String filter) {
        return buildUri(URI, ORDER_ENTRY_SET_NAME, filter, ORDER_EXPAND_FIELDS, ORDER_SELECT_FIELDS);
    }

    URI buildProductUri(String filter) {
        return buildUri(URI, PRODUCT_ENTRY_SET_NAME, filter, PRODUCT_EXPAND_FIELDS, PRODUCT_SELECT_FIELDS);
    }

    private URI buildUri(String serviceUri, String entitySetName, String filter, String expand, String select) {
        return client.newURIBuilder(serviceUri)
                .appendEntitySetSegment(entitySetName)
                .filter(filter)
                .expand(expand)
                .select(select)
                .format("json")
                .build();
    }

    /*Parse olingo client values*/
    private String parseComplex(ClientValue value) {
        return (String) value
                .asComplex()
                .asJavaMap()
                .values()
                .toArray()[0];
    }

    private <T> T parsePrimitive(ClientValue value) {
        ClientPrimitiveValue clientPrimitiveValue = value.asPrimitive();
        Class<?> clazz = clientPrimitiveValue.getType().getDefaultType();

        try {
            return (T) clientPrimitiveValue.toCastValue(clazz);
        } catch (EdmPrimitiveTypeException exception) {
            throw new RuntimeException(String.format("Error during parsing primitive type - %s", exception));
        }
    }

    private LocalDate parseDate(ClientValue date) {
        return LocalDateTime
                .parse(parsePrimitive(date))
                .toLocalDate();
    }

    /*Filters*/
    private String createProductsFilter(List<String> refKeys) {
        StringJoiner stringJoiner = new StringJoiner(" or ");
        refKeys.forEach(
                key -> stringJoiner.add(createRefKeyFilterRequest(key))
        );

        return stringJoiner.toString();
    }
    private String createRefKeyFilterRequest(String key) {
        return String.format("Ref_Key eq guid'%s'", key);
    }

    private String createDateFilter(String date) {
        return String.format("ДатаОтгрузки eq datetime'%s'", date);
    }
}