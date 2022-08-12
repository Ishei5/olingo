package org.apache.olingo.test;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.*;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.client.core.domain.ClientEntityImpl;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MyExample {

    private static final String ORDER_ENTRY_SET_NAME = "Document_ЗаказПокупателя";
    private static final String ORDER_SELECT_FIELDS = "Ref_Key,Number,Date,АдресДоставки,Организация/Description,Ответственный/Code";
    private static final String ORDER_EXPAND_FIELDS = "Ответственный/ФизЛицо,Организация";
    private static final String PRODUCT_ENTRY_SET_NAME = "Document_ЗаказПокупателя_Товары";
    private static final String PRODUCT_SELECT_FIELDS = "Количество,КоличествоМест,Коэффициент,ЕдиницаИзмерения/Description,Номенклатура/Description";
    private static final String PRODUCT_EXPAND_FIELDS = "Номенклатура,ЕдиницаИзмерения";

    public static final String URI = "http://89.76.239.245:8080/test/odata/standard.odata/";
//    public static final String URI = "http://localhost:8000/zakaz_select.json";
//    public static final String URI = "http://localhost:8000/tovari.json";
    private final ODataClient client;

    public MyExample() {
        this.client = ODataClientFactory.getClient();
    }

    public static void main(String[] args) {
        MyExample myExample = new MyExample();
        myExample.performe(URI);
    }

    private void performe(String uri) {
//        (URI, "Document_ЗаказПокупателя", "Ref_Key eq guid'fdc3291b-5e10-11ea-b5ce-94de80dde3f4'");
        URI orderUri = buildUri(URI, ORDER_ENTRY_SET_NAME, createDateRequest("2020-03-05T00:00:00"), ORDER_EXPAND_FIELDS, ORDER_SELECT_FIELDS);
        ClientEntitySet clientEntitySet = readEntities(orderUri);
        List<Map<String, ClientValue>> clientMap = clientEntitySetToListOfMap(clientEntitySet);
        List<OrderDto> orderDtos = clientMap.stream()
                .map(orderMap -> parseOrder(orderMap))
                .toList();
        System.out.println();
//        clientMap.stream().map(clientMap -> parseOrder());
//        ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = readEntities(orderUri);
//        while (iterator.hasNext()) {
//            ClientEntity ce = iterator.next();
//            parseOrder(ce.getProperties());
//        }
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

//    private ClientEntitySetIterator<ClientEntitySet, ClientEntity> readEntities(URI absoluteUri) {
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

        URI productUri = buildUri(URI, PRODUCT_ENTRY_SET_NAME, createRefKeyFilterRequest(refKey), PRODUCT_EXPAND_FIELDS, PRODUCT_SELECT_FIELDS);
        ClientEntitySet clientEntitySet = readEntities(productUri);
        List<Map<String, ClientValue>> productsMap = clientEntitySetToListOfMap(clientEntitySet);
        List<ProductDto> products = productsMap.stream()
                .map(this::parseProduct)
                .collect(Collectors.toList());

        OrderDto orderDto = OrderDto.builder()
                .orderNumber(parsePrimitive(orderProperties.get("Ref_Key")))
                .createdDate(parseDate(orderProperties.get("Date")))
                .address(parsePrimitive(orderProperties.get("АдресДоставки")))
                .products(products)
                .clientName(parseComplex(orderProperties.get("Организация")))
                .managerFullName(parseComplex(orderProperties.get("Ответственный")).trim())
                .build();

        return orderDto;
    }

    private void parseProducts(ClientEntitySetIterator<ClientEntitySet, ClientEntity> productIterator) {

        /*List<ProductDto> list = new ArrayList<>(1);
        while (productIterator.hasNext()) {
            ClientEntity clientEntity = productIterator.next();
            ProductDto productDto = parseProduct(clientEntity.getProperties());
            list.add(productDto);
        }

        return list;*/
    }

    private ProductDto parseProduct(Map<String, ClientValue> productProperties) {
        Integer amount = Integer.parseInt(parsePrimitive(productProperties.get("КоличествоМест")));
        Double totalWeight = Double.valueOf(parsePrimitive(productProperties.get("Количество")).toString());

        ProductDto productDto = ProductDto.builder()
                .productName(parseComplex(productProperties.get("Номенклатура")))
                .amount(amount)
                .unitWeight(totalWeight / amount)
                .totalProductWeight(totalWeight)
                .build();

        return productDto;
    }
}