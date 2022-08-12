package org.apache.olingo.test;

import org.apache.olingo.client.api.domain.ClientPrimitiveValue;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.mapstruct.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

@Mapper(componentModel = "spring")
//@Mapper
public interface Entity1CMapper {

    @Mapping(target = "orderNumber", source = "Number", qualifiedByName = "primitive")
    @Mapping(target = "createdDate", source = "Date", qualifiedByName = "date")
    @Mapping(target = "clientName", source = "Организация", qualifiedByName = "complex")
    @Mapping(target = "address", source = "АдресДоставки", qualifiedByName = "primitive")
    @Mapping(target = "managerFullName", source = "Ответственный", qualifiedByName = "complex")
    @Mapping(target = "orderWeight", constant  = "1.0")
    @Mapping(target = "id", ignore = true)
    @Mapping(target = "products", ignore = true)
    OrderDto order1CPropertiesToOrderDto(Map<String, ClientValue> orderProperties);

    /*@Mapping(target = "orderNumber", source = "Номенклатура", qualifiedByName = "complex")
    @Mapping(target = "amount", source = "КоличествоМест", qualifiedByName = "parseInt")
//    @Mapping(target = "storeName", source = "АдресДоставки", qualifiedByName = "primitive")
    @Mapping(target = "unitWeight", source = "Ответственный", qualifiedByName = "complex")
    @Mapping(target = "totalProductWeight", source = "Ответственный", qualifiedByName = "complex")
    @Mapping(target = "id", ignore = true)
    ProductDto product1CPropertiesToProductDto(Map<String, ClientValue> productProperties);*/

    @AfterMapping
    default void setUnitWeight(@MappingTarget ProductDto productDto) {
        Double unitWeight = productDto.getTotalProductWeight() / productDto.getAmount();
        productDto.setUnitWeight(unitWeight);
    }

    @Named("complex")
    default String parseComplex(ClientValue value) {
        return (String) value
                .asComplex()
                .asJavaMap()
                .values()
                .toArray()[0];
    }

    @Named("primitive")
    default <T> T parsePrimitive(ClientValue value) {
        ClientPrimitiveValue clientPrimitiveValue = value.asPrimitive();
        Class<?> clazz = clientPrimitiveValue.getType().getDefaultType();

        try {
            return (T) clientPrimitiveValue.toCastValue(clazz);
        } catch (EdmPrimitiveTypeException exception) {
            throw new RuntimeException(String.format("Error during parsing primitive type - %s", exception));
        }
    }

    @Named("date")
    default LocalDate parseDate(ClientValue date) {
        return LocalDateTime
                .parse(parsePrimitive(date))
                .toLocalDate();
    }

    @Named("parseInt")
    default Integer parseInt(ClientValue value) {
        return Integer.parseInt(parsePrimitive(value));
    }

    @Named("totalWeight")
    default Double getTotalWeight(ClientValue value) {
        return Double.valueOf(parsePrimitive(value).toString());
    }
}
