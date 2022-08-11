package org.apache.olingo.test;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.*;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
        ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = readEntities(orderUri);
        while (iterator.hasNext()) {
            ClientEntity ce = iterator.next();
            parseOrder(ce.getProperties());
        }
    }

    public URI buildUri(String serviceUri, String entitySetName, String filter, String expand, String select) {

        return client.newURIBuilder(serviceUri)
                .appendEntitySetSegment(entitySetName)
                .filter(filter)
                .expand(expand)
                .select(select)
                .build();
    }

    private ClientEntitySetIterator<ClientEntitySet, ClientEntity> readEntities(URI absoluteUri) {
        System.out.println("URI = " + java.net.URLDecoder.decode(String.valueOf(absoluteUri), StandardCharsets.UTF_8));
        ODataEntitySetIteratorRequest<ClientEntitySet, ClientEntity> request =
                client.getRetrieveRequestFactory().getEntitySetIteratorRequest(absoluteUri);
        request.setAccept("application/json");
        ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response = request.execute();

        return response.getBody();
    }

    private String parseComplex(ClientProperty property) {
        return (String) property.getValue()
                .asComplex()
                .asJavaMap()
                .values()
                .toArray()[0];
    }

    private <T> T parsePrimitive(ClientProperty property) {
        ClientPrimitiveValue clientPrimitiveValue = property.getValue().asPrimitive();
        Class<?> clazz = clientPrimitiveValue.getType().getDefaultType();

        try {
            return (T) clientPrimitiveValue.toCastValue(clazz);
        } catch (EdmPrimitiveTypeException exception) {
            throw new RuntimeException(String.format("Error during parsing primitive type - %s", exception));
        }
    }

    private String createRefKeyFilterRequest(String key) {
        return String.format("Ref_Key eq guid'%s'", key);
    }

    private String createDateRequest(String date) {
        return String.format("ДатаОтгрузки eq datetime'%s'", date);
    }

    private LocalDate parseDate(ClientProperty property) {
        return LocalDateTime
                .parse(parsePrimitive(property))
                .toLocalDate();
    }

    private OrderDto parseOrder(Collection<ClientProperty> orderProperties) {
        List<ClientProperty> clientProperties = orderProperties.stream().toList();
        String refKey = parsePrimitive(clientProperties.get(0));

        URI productUri = buildUri(URI, PRODUCT_ENTRY_SET_NAME, createRefKeyFilterRequest(refKey), PRODUCT_EXPAND_FIELDS, PRODUCT_SELECT_FIELDS);
        ClientEntitySetIterator<ClientEntitySet, ClientEntity> productIterator = readEntities(productUri);
        List<ProductDto> products = parseProducts(productIterator);

        OrderDto orderDto = OrderDto.builder()
                .orderNumber(parsePrimitive(clientProperties.get(1)))
                .createdDate(parseDate(clientProperties.get(2)))
                .address(parsePrimitive(clientProperties.get(3)))
                .products(products)
                .clientName(parseComplex(clientProperties.get(5)))
                .managerFullName(parseComplex(clientProperties.get(7)).trim())
                .build();
        System.out.println(orderDto);

        return orderDto;
    }

    private List<ProductDto> parseProducts(ClientEntitySetIterator<ClientEntitySet, ClientEntity> productIterator) {
        List<ProductDto> list = new ArrayList<>(1);
        while (productIterator.hasNext()) {
            ClientEntity clientEntity = productIterator.next();
            ProductDto productDto = parseProduct(clientEntity.getProperties());
            list.add(productDto);
        }

        return list;
    }

    private ProductDto parseProduct(Collection<ClientProperty> productProperties) {
        List<ClientProperty> clientProperties = productProperties.stream().toList();
        Integer amount = Integer.parseInt(parsePrimitive(clientProperties.get(1)));
        Double totalWeight = Double.valueOf(parsePrimitive(clientProperties.get(0)).toString());

        ProductDto productDto = ProductDto.builder()
                .productName(parseComplex(clientProperties.get(6)))
                .amount(amount)
                .unitWeight(totalWeight / amount)
                .totalProductWeight(totalWeight)
                .build();

        return productDto;
    }
}