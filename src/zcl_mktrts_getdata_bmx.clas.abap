CLASS zcl_mktrts_getdata_bmx DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    TYPES:BEGIN OF ty_comm_para,
            name  TYPE if_com_scenario=>ty_cscn_property-name,
            value TYPE c LENGTH 512,
          END OF ty_comm_para,
          ty_t_comm_para TYPE STANDARD TABLE OF ty_comm_para.

    TYPES:BEGIN OF ty_log_msg.
            .INCLUDE TYPE bapiret2.
    TYPES:  status TYPE c LENGTH 1,
          END OF ty_log_msg.

    TYPES:
      BEGIN OF ty_datos,
        fecha TYPE string,
        dato  TYPE string,
      END OF ty_datos,

      BEGIN OF ty_series,
        idserie TYPE string,
        titulo  TYPE string,
        datos   TYPE TABLE OF ty_datos WITH EMPTY KEY,
      END OF ty_series,

      BEGIN OF ty_bmx,
        BEGIN OF bmx,
          series TYPE TABLE OF ty_series WITH EMPTY KEY,
        END OF bmx,
      END OF ty_bmx.

    TYPES:
      BEGIN OF ty_usd,
        usd TYPE string,
      END OF ty_usd,

      BEGIN OF ty_ffrx_data,
        base   TYPE string,
        result TYPE ty_usd,
      END OF ty_ffrx_data.

    TYPES:
      BEGIN OF ty_upload_data,
        providerCode       TYPE string,
        marketDataSource   TYPE string,
        marketDataCategory TYPE string,
        key1               TYPE string,
        key2               TYPE string,
        marketDataProperty TYPE string,
        effectiveDate      TYPE string,
        effectiveTime      TYPE string,
        marketDataValue    TYPE string,
        securityCurrency   TYPE string,
        fromFactor         TYPE string,
        toFactor           TYPE string,
        priceQuotation     TYPE string,
        additionalKey      TYPE string,
      END OF ty_upload_data.

    INTERFACES if_oo_adt_classrun .

    METHODS get_comm_parameter
      IMPORTING
        iv_comm_arr     TYPE ty_comm_para-name
      EXPORTING
        ev_comm_out_srv TYPE if_com_management=>ty_cscn_outb_srv_id
        et_comm_para    TYPE ty_t_comm_para.

    METHODS get_bmx_data
      EXPORTING
        et_rates_data TYPE ty_bmx.

    METHODS get_ffrx_data
      EXPORTING
        et_rates_data TYPE ty_ffrx_data.

    METHODS post_rates
      IMPORTING
        et_rates_data TYPE ty_bmx.

    METHODS post_rates_ffrx
      IMPORTING
        et_rates_data TYPE ty_ffrx_data.

    CLASS-DATA:
      gr_bali_log TYPE REF TO            if_bali_log,
      gt_log_msg  TYPE STANDARD TABLE OF ty_log_msg.

  PROTECTED SECTION.
  PRIVATE SECTION.



ENDCLASS.



CLASS zcl_mktrts_getdata_bmx IMPLEMENTATION.


  METHOD if_oo_adt_classrun~main.

    get_bmx_data( IMPORTING et_rates_data = DATA(lv_rates) ).

    out->write( 'Retrieved Data from Banxico' ).
    out->write( lv_rates ).

    post_rates( et_rates_data = lv_rates ).

    out->write( 'Uploaded Data from Banxico' ).

    get_ffrx_data( IMPORTING et_rates_data = DATA(lv_rates_ffrx) ).

    out->write( 'Retrieved Data from FastForex' ).
    out->write( lv_rates_ffrx ).

    post_rates_ffrx( et_rates_data = lv_rates_ffrx ).

    out->write( 'Uploaded Data from FastForex' ).


  ENDMETHOD.

  METHOD get_bmx_data.

    get_comm_parameter( EXPORTING iv_comm_arr = 'ZBMX_GET_RATES'
                        IMPORTING ev_comm_out_srv = DATA(lv_comm_out_srv)
                                     et_comm_para = DATA(lt_comm_arrng) ).

    DATA(ls_bmxtoken) = VALUE #( lt_comm_arrng[ name = 'bmx-token' ] ).

    IF ls_bmxtoken IS NOT INITIAL.

      TRY.
          DATA(lo_destination) = cl_http_destination_provider=>create_by_comm_arrangement(
                                   comm_scenario  = 'ZBMX_GET_RATES'
                                   service_id     = 'ZSRV_BMX_RATES_REST'
                                 ).

          DATA(lo_http_client) = cl_web_http_client_manager=>create_by_http_destination( i_destination = lo_destination ).
          DATA(lo_request) = lo_http_client->get_http_request( ).

          "adding headers
          lo_request->set_header_fields( VALUE #(
                                                          (  name = 'Bmx-Token' value = ls_bmxtoken-value )
                                                          (  name = 'Accept' value = '*/*' )
                                                          (  name = 'Accept-Encoding' value = 'gzip, deflate, br' )
                                                          ) ).

          "set request method and execute request
          DATA(lo_web_http_response) = lo_http_client->execute( if_web_http_client=>get ).
          DATA(lv_response) = lo_web_http_response->get_text( ).

          /ui2/cl_json=>deserialize(
            EXPORTING
              json             = lv_response
              pretty_name      = /ui2/cl_json=>pretty_mode-camel_case
            CHANGING
              data             = et_rates_data
          ).

        CATCH cx_root INTO DATA(lx_exception).
*        out->write( lx_exception->get_text( ) ).
      ENDTRY.

    ENDIF.

  ENDMETHOD.

  METHOD get_ffrx_data.

    get_comm_parameter( EXPORTING iv_comm_arr = 'ZFFRX_GET_RATES'
                        IMPORTING ev_comm_out_srv = DATA(lv_comm_out_srv)
                                     et_comm_para = DATA(lt_comm_arrng) ).

    DATA(ls_ffrxkey) = VALUE #( lt_comm_arrng[ name = 'api_key' ] ).

    IF ls_ffrxkey IS NOT INITIAL.

      TRY.
          DATA(lo_destination) = cl_http_destination_provider=>create_by_comm_arrangement(
                                   comm_scenario  = 'ZFFRX_GET_RATES'
                                   service_id     = 'ZSRV_FASTFOREX_RATES_REST'
                                 ).

          DATA(lo_http_client) = cl_web_http_client_manager=>create_by_http_destination( i_destination = lo_destination ).
          DATA(lo_request) = lo_http_client->get_http_request( ).

          lo_request->set_form_fields( VALUE #(
                                                          (  name = 'from' value = 'EUR' )
                                                          (  name = 'to' value = 'USD' )
                                                          (  name = 'api_key' value = ls_ffrxkey-value )
                                                          ) ).

          "adding headers
          lo_request->set_header_fields( VALUE #(
                                                          (  name = 'Accept' value = '*/*' )
                                                          (  name = 'Accept-Encoding' value = 'gzip, deflate, br' )
                                                          ) ).

          "set request method and execute request
          DATA(lo_web_http_response) = lo_http_client->execute( if_web_http_client=>get ).
          DATA(lv_response) = lo_web_http_response->get_text( ).

          /ui2/cl_json=>deserialize(
            EXPORTING
              json             = lv_response
              pretty_name      = /ui2/cl_json=>pretty_mode-camel_case
            CHANGING
              data             = et_rates_data
          ).

        CATCH cx_root INTO DATA(lx_exception).
*        out->write( lx_exception->get_text( ) ).
      ENDTRY.

    ENDIF.

  ENDMETHOD.

  METHOD post_rates.
    "get data from struct
    DATA: lv_rates    TYPE ty_series,
          lv_datos    TYPE ty_datos,
          lv_date     TYPE sy-datlo,
          lv_date_ext TYPE string,
          lv_time     TYPE sy-timlo,
          lv_time_ext TYPE string.

    READ TABLE et_rates_data-bmx-series INTO lv_rates INDEX 1.
    READ TABLE lv_rates-datos INTO lv_datos INDEX 1.

    lv_date =  sy-datlo.

    TRY.
        CALL METHOD cl_abap_datfm=>conv_date_int_to_ext
          EXPORTING
            im_datint   = lv_date
            im_datfmdes = '6'
          IMPORTING
            ex_datext   = lv_date_ext.
      CATCH cx_abap_datfm_format_unknown.
    ENDTRY.

    lv_time = sy-timlo.

    TRY.
        CALL METHOD cl_abap_timefm=>conv_time_int_to_ext
          EXPORTING
            time_int            = lv_time
*           without_seconds     = abap_false
            format_according_to = 1
          IMPORTING
            time_ext            = lv_time_ext.
      CATCH cx_parameter_invalid_range.
    ENDTRY.

    "create request body
    DATA(lv_json) = '[' && |\n|  &&
                    '  {' && |\n|  &&
                    '    "providerCode": "ZBMX",' && |\n|  &&
                    '    "marketDataSource": "BYOR",' && |\n|  &&
                    '    "marketDataCategory": "01",' && |\n|  &&
                    '    "key1": "USD",' && |\n|  &&
                    '    "key2": "MXN",' && |\n|  &&
                    '    "marketDataProperty": "MID",' && |\n|  &&
                    '    "effectiveDate": "' && lv_date_ext && '",' && |\n|  &&
                    '    "effectiveTime": "' && lv_time_ext && '",' && |\n|  &&
                    '    "marketDataValue": ' && lv_datos-dato && ',' && |\n|  &&
                    '    "securityCurrency": "MXN",' && |\n|  &&
                    '    "fromFactor": 1,' && |\n|  &&
                    '    "toFactor": 1,' && |\n|  &&
                    '    "priceQuotation": null,' && |\n|  &&
                    '    "additionalKey": null' && |\n|  &&
                    '  }' && |\n|  &&
                    ']'.
    "call post operation
    TRY.
        DATA(lo_destination) = cl_http_destination_provider=>create_by_comm_arrangement(
                                 comm_scenario  = 'ZMKT_POST_RATES'
                                 service_id     = 'ZSRV_MRKTRTS_UPLOAD_REST'

                               ).

        DATA(lo_http_client) = cl_web_http_client_manager=>create_by_http_destination( i_destination = lo_destination ).
        DATA(lo_request) = lo_http_client->get_http_request( ).


        "adding headers
        lo_request->set_header_fields( VALUE #(
                                                        (  name = 'Content-Type' value = 'application/json' )
                                                        ) ).

        lo_request->append_text(
          EXPORTING
            data   = lv_json
        ).

        "set request method and execute request
        DATA(lo_web_http_response) = lo_http_client->execute( if_web_http_client=>post ).
        DATA(lv_response) = lo_web_http_response->get_text( ).

      CATCH cx_root INTO DATA(lx_exception).

    ENDTRY.
  ENDMETHOD.

  METHOD post_rates_ffrx.
    "get data from struct
    DATA: lv_rates    TYPE string,
          lv_date     TYPE dats,
          lv_date_ext TYPE string,
          lv_time     TYPE sy-timlo,
          lv_time_ext TYPE string.

    lv_rates = et_rates_data-result-usd.
    lv_date = sy-datlo.

    TRY.
        CALL METHOD cl_abap_datfm=>conv_date_int_to_ext
          EXPORTING
            im_datint   = lv_date
            im_datfmdes = '6'
          IMPORTING
            ex_datext   = lv_date_ext.
      CATCH cx_abap_datfm_format_unknown.
    ENDTRY.

    lv_time = sy-timlo.

    TRY.
        CALL METHOD cl_abap_timefm=>conv_time_int_to_ext
          EXPORTING
            time_int            = lv_time
*           without_seconds     = abap_false
            format_according_to = 1
          IMPORTING
            time_ext            = lv_time_ext.
      CATCH cx_parameter_invalid_range.
    ENDTRY.

    "create request body
    DATA(lv_json) = '[' && |\n|  &&
                    '  {' && |\n|  &&
                    '    "providerCode": "ZFFRX",' && |\n|  &&
                    '    "marketDataSource": "BYOR",' && |\n|  &&
                    '    "marketDataCategory": "01",' && |\n|  &&
                    '    "key1": "EUR",' && |\n|  &&
                    '    "key2": "USD",' && |\n|  &&
                    '    "marketDataProperty": "MID",' && |\n|  &&
                    '    "effectiveDate": "' && lv_date_ext && '",' && |\n|  &&
                    '    "effectiveTime": "' && lv_time_ext && '",' && |\n|  &&
                    '    "marketDataValue": ' && lv_rates && ',' && |\n|  &&
                    '    "securityCurrency": "USD",' && |\n|  &&
                    '    "fromFactor": 1,' && |\n|  &&
                    '    "toFactor": 1,' && |\n|  &&
                    '    "priceQuotation": null,' && |\n|  &&
                    '    "additionalKey": null' && |\n|  &&
                    '  }' && |\n|  &&
                    ']'.
    "call post operation
    TRY.
        DATA(lo_destination) = cl_http_destination_provider=>create_by_comm_arrangement(
                                 comm_scenario  = 'ZMKT_POST_RATES'
                                 service_id     = 'ZSRV_MRKTRTS_UPLOAD_REST'

                               ).

        DATA(lo_http_client) = cl_web_http_client_manager=>create_by_http_destination( i_destination = lo_destination ).
        DATA(lo_request) = lo_http_client->get_http_request( ).


        "adding headers
        lo_request->set_header_fields( VALUE #(
                                                        (  name = 'Content-Type' value = 'application/json' )
                                                        ) ).

        lo_request->append_text(
          EXPORTING
            data   = lv_json
        ).

        "set request method and execute request
        DATA(lo_web_http_response) = lo_http_client->execute( if_web_http_client=>post ).
        DATA(lv_response) = lo_web_http_response->get_text( ).

      CATCH cx_root INTO DATA(lx_exception).

    ENDTRY.
  ENDMETHOD.

  METHOD get_comm_parameter.
    DATA: lr_cscn TYPE if_com_scenario_factory=>ty_query-cscn_id_range,
          lt_prpn TYPE if_com_arrangement_factory=>ty_query-ca_property.


    lr_cscn = VALUE #( ( sign = 'I' option = 'EQ' low = iv_comm_arr ) ).

    DATA(lr_factory) = cl_com_arrangement_factory=>create_instance( ).

    "GET Comm. Arrangement Data
    lr_factory->query_ca(
      EXPORTING
        is_query = VALUE #( cscn_id_range = lr_cscn
                            ca_property = lt_prpn
                            )
            IMPORTING
                et_com_arrangement = DATA(lt_comm_sc) ).

    IF lt_comm_sc IS NOT INITIAL. "data found
      READ TABLE lt_comm_sc ASSIGNING FIELD-SYMBOL(<lr_comm_sc>) INDEX 1.
      IF sy-subrc = 0.
        DATA(lt_properties) = <lr_comm_sc>->get_properties( ).
        DATA(lt_outbound)   = <lr_comm_sc>->get_outbound_services( ).
        ev_comm_out_srv     = VALUE #( lt_outbound[ 1 ]-id OPTIONAL ).

        LOOP AT lt_properties ASSIGNING FIELD-SYMBOL(<ls_property>).
          TRANSLATE <ls_property>-name TO LOWER CASE.
          DATA(lv_prop_name) = <ls_property>-name.

          DATA(lv_prop_value) = VALUE #( <ls_property>-values[ 1 ] OPTIONAL ).
          SHIFT lv_prop_value RIGHT DELETING TRAILING '*'.
          APPEND VALUE #( name = lv_prop_name value = lv_prop_value ) TO et_comm_para.

        ENDLOOP.
      ENDIF.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
