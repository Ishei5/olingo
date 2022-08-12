package org.apache.olingo.test;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.*;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MyExample {

    private static final String ORDER_ENTRY_SET_NAME = "Document_ЗаказПокупателя";
    private static final String ORDER_SELECT_FIELDS = "Ref_Key,Number,Date,АдресДоставки,Контрагент/Description,Ответственный/Code";
    private static final String ORDER_EXPAND_FIELDS = "Ответственный/ФизЛицо,Контрагент";
    private static final String PRODUCT_ENTRY_SET_NAME = "Document_ЗаказПокупателя_Товары";
    private static final String PRODUCT_SELECT_FIELDS = "Количество,КоличествоМест,Коэффициент,ЕдиницаИзмерения/Description,Номенклатура/Description";
    private static final String PRODUCT_EXPAND_FIELDS = "Номенклатура,ЕдиницаИзмерения";

    public static final String URI = "http://89.76.239.245:8080/test/odata/standard.odata/";
    private final ODataClient client;

    public MyExample() {
        this.client = ODataClientFactory.getClient();
    }

    public static void main(String[] args) {
        MyExample myExample = new MyExample();
        myExample.performe(URI);
    }

    private void performe(String uri) {
        URI orderUri = buildUri(uri, ORDER_ENTRY_SET_NAME, createDateRequest("2020-03-05T00:00:00"), ORDER_EXPAND_FIELDS, ORDER_SELECT_FIELDS);
        ClientEntitySet clientEntitySet = readEntities(orderUri);
        List<Map<String, ClientValue>> clientMap = clientEntitySetToListOfMap(clientEntitySet);
        List<OrderDto> orderDtos = clientMap.stream()
                .map(orderMap -> parseOrder(orderMap))
                .toList();
    }

    private List<Map<String, ClientValue>> clientEntitySetToListOfMap(ClientEntitySet clientEntitySet) {
        return clientEntitySet.getEntities().stream()
                .map(ClientEntity::getProperties)
                .map(clientProperties -> clientProperties.stream()
                        .collect(Collectors.toMap(ClientProperty::getName, ClientProperty::getValue)))
                .toList();
    }

    public URI buildUri(String serviceUri, String entitySetName, String filter, String expand, String select) {

        return client.newURIBuilder(serviceUri)
                .appendEntitySetSegment(entitySetName)
                .filter(filter)
                .expand(expand)
                .select(select)
                .build();
    }

    private ClientEntitySet readEntities(URI absoluteUri) {
        System.out.println("URI = " + java.net.URLDecoder.decode(String.valueOf(absoluteUri), StandardCharsets.UTF_8));
        ODataEntitySetRequest<ClientEntitySet> request = client.getRetrieveRequestFactory().getEntitySetRequest(absoluteUri);
        request.setAccept("application/json");
        ODataRetrieveResponse<ClientEntitySet> response = request.execute();

        return response.getBody();
    }

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

    private String createRefKeyFilterRequest(String key) {
        return String.format("Ref_Key eq guid'%s'", key);
    }

    private String createDateRequest(String date) {
        return String.format("ДатаОтгрузки eq datetime'%s'", date);
    }


    private OrderDto parseOrder(Map<String, ClientValue> orderProperties) {
        String refKey = parsePrimitive(orderProperties.get("Ref_Key"));
        System.out.println(refKey);
        URI productUri = buildUri(URI, PRODUCT_ENTRY_SET_NAME, createRefKeyFilterRequest(refKey), PRODUCT_EXPAND_FIELDS, PRODUCT_SELECT_FIELDS);
        ClientEntitySet clientEntitySet = readEntities(productUri);
        List<Map<String, ClientValue>> productsMap = clientEntitySetToListOfMap(clientEntitySet);
        List<ProductDto> products = productsMap.stream()
                .map(this::parseProduct)
                .collect(Collectors.toList());

        OrderDto build = OrderDto.builder()
                .orderNumber(parsePrimitive(orderProperties.get("Ref_Key")))
                .createdDate(parseDate(orderProperties.get("Date")))
                .address(parsePrimitive(orderProperties.get("АдресДоставки")))
                .products(products)
                .clientName(parseComplex(orderProperties.get("Контрагент")))
                .managerFullName(parseComplex(orderProperties.get("Ответственный")).trim())
                .orderWeight(getTotalOrderWeight(products))
                .build();

        return build;
    }

    private double getTotalOrderWeight(List<ProductDto> products) {
        return products.stream()
                .mapToDouble(ProductDto::getTotalProductWeight)
                .sum();
    }

    private ProductDto parseProduct(Map<String, ClientValue> productProperties) {
        List<String> allowableMeasure = List.of("кг", "л", "шт");
        int amount = Integer.parseInt(parsePrimitive(productProperties.get("КоличествоМест")));
        String measure = parseComplex(productProperties.get("ЕдиницаИзмерения"));
        double totalWeight = allowableMeasure.contains(measure) ?
                (!measure.equals("шт")) ? Double.parseDouble(parsePrimitive(productProperties.get("Количество")).toString()) : 1.0
                 : 0.0;
        String storeName = "Склад №1";

        return ProductDto.builder()
                .productName(parseComplex(productProperties.get("Номенклатура")))
                .amount(amount)
                .unitWeight(totalWeight / amount)
                .totalProductWeight(totalWeight)
                .storeName(storeName)
                .build();
    }
}