
CREATE TABLE TRANSACTION_IDENTIFIER
(
  APP_NAME              VARCHAR2(100 BYTE), 
  CATEGORY              VARCHAR2(25 BYTE),
  CHANGE_INDICATOR      VARCHAR2(20 BYTE),	
  DEL_MEDIUM            VARCHAR2(30 BYTE),
  DISP_TEMPLATE_NAME    VARCHAR2(200 BYTE),
  LEAD_TYPE             VARCHAR2(5 BYTE),
  LOB                   VARCHAR2(25 BYTE),
  MARKETING_ID          VARCHAR2(30 BYTE),
  OPEN_DATE             DATE,
  OPENED                NUMBER DEFAULT 0,
  OPT_OUT               NUMBER,
  ORIG_SRC_SYSTEM       VARCHAR2(200 BYTE),
  ROLE_ID               VARCHAR2(100 BYTE),
  SENT_DATE             DATE,
  SEQ_ID                NUMBER                  DEFAULT 0,
  STATE                 VARCHAR2(30 BYTE),
  STATUS_ID             NUMBER,
  SUBJECT               VARCHAR2(4000 BYTE),
  SYSTEM_SERVICE_STATE  VARCHAR2(10 BYTE),
  TEMPLATE_ID           VARCHAR2(40 BYTE),
  TEMPLATE_NAME         VARCHAR2(200 BYTE),
  TEMPLATE_SEQ_ID       NUMBER,
  TEMPLATE_VER          VARCHAR2(40 BYTE),
  TX_TYPE               VARCHAR2(25 BYTE),	
  UNIQUE_CLICKS         NUMBER,
  UNIQUE_OPENS          NUMBER
 )
COMPRESS FOR OLTP 
TABLESPACE PNPDATA_TI1
PCTUSED    0
PCTFREE    10
INITRANS   20
MAXTRANS   255
STORAGE    (
            BUFFER_POOL      DEFAULT
           )
LOGGING
PARTITION BY RANGE (SENT_DATE)
(  
  PARTITION PTI_MAX VALUES LESS THAN (MAXVALUE)
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   20
    MAXTRANS   255
    STORAGE    (
                INITIAL          8M
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
)
NOCACHE
MONITORING
ENABLE ROW MOVEMENT;

CREATE INDEX INPUT_ID_AV_IDX ON TRANSACTION_IDENTIFIER
(INPUT_ID)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TEMPLATE_ID_AV_IDX ON TRANSACTION_IDENTIFIER
(TEMPLATE_ID)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TI_TOADD_STDT1_INDX ON TRANSACTION_IDENTIFIER
(TO_ADDRESS, SENT_DATE)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE UNIQUE INDEX TI_TX_APP_SEQ_IDX ON TRANSACTION_IDENTIFIER
(TX_ID, APP_NAME, SEQ_ID)
LOGGING
TABLESPACE PNPINDX
PCTFREE    10
INITRANS   2
MAXTRANS   255
STORAGE    (
            INITIAL          64K
            NEXT             1M
            MAXSIZE          UNLIMITED
            MINEXTENTS       1
            MAXEXTENTS       UNLIMITED
            PCTINCREASE      0
            BUFFER_POOL      DEFAULT
           );

CREATE INDEX TRANS_AMIT_BAN1_INDX ON TRANSACTION_IDENTIFIER
(BCAN)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_BTN_INDX ON TRANSACTION_IDENTIFIER
(BTN)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_CORP_INDX ON TRANSACTION_IDENTIFIER
(CORP_ID)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_EMAIL_INDX ON TRANSACTION_IDENTIFIER
(TO_ADDRESS)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_ENT_INDX ON TRANSACTION_IDENTIFIER
(ENT_ID)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_FAX_INDX ON TRANSACTION_IDENTIFIER
(FAX)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_LOB_INDX ON TRANSACTION_IDENTIFIER
(LOB)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_MTN_INDX ON TRANSACTION_IDENTIFIER
(MTN)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_PCAN_INDX ON TRANSACTION_IDENTIFIER
(PCAN)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_SDT_INDX ON TRANSACTION_IDENTIFIER
(SENT_DATE)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE INDEX TRANS_AMIT_SPM_INDX ON TRANSACTION_IDENTIFIER
(SPM_ID)
  PCTFREE    10
  INITRANS   2
  MAXTRANS   255
  STORAGE    (
              BUFFER_POOL      DEFAULT
             )
LOCAL (  
  PARTITION PTI_MAX
    LOGGING
    NOCOMPRESS 
    TABLESPACE PNPDATA_TI4
    PCTFREE    10
    INITRANS   2
    MAXTRANS   255
    STORAGE    (
                INITIAL          64K
                NEXT             1M
                MAXSIZE          UNLIMITED
                MINEXTENTS       1
                MAXEXTENTS       UNLIMITED
                BUFFER_POOL      DEFAULT
               )
);

CREATE OR REPLACE PUBLIC SYNONYM TRANSACTION_IDENTIFIER FOR TRANSACTION_IDENTIFIER;


