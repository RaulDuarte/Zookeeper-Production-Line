package com.raulduarte.config;

public final class Configuration {

    //  General
    
    public final static long    PIECE_MACH     = 50;            //Número de peças que produzidas

    public final static long    VELOCITY       = 800;          //Tempo de atualização de cada thread

    public final static String  ADDRESS        = "127.0.0.1";   //Endereço do servidor

    public final static int     FAILURE_RATE   = 10;            //Probabilidade de peças defeitusas

    public final static int     PACKAGE_SIZE   = 5;            //Quantidade de itens em cada pacote
    

    //  Leader
    
    public final static String  ROOT_LEADER    = "/production_line/election";


    // Root Path

    public final static String  ROOT           = "/production_line";


    //  Manufactured Item Path

    public final static String manufactured_item      = "/production_line/manufactured_item"; 


    //  Signal Path

    public final static String  ROOT_SIGNAL    = "/production_line/thread_signal";
    

    //  Sensor Paths

    public final static String  SIGNAL_SENSOR  = "/production_line/thread_signal/sensor";

    public final static String  ROOT_SENSOR    = "/production_line/sensor";


    //  Discard Paths

    public final static String  SIGNAL_DISCARD = "/production_line/thread_signal/discard";

    public final static String  ROOT_DISCARD   = "/production_line/discard";


    //  Tag Paths

    public final static String  SIGNAL_TAG     = "/production_line/thread_signal/tag";

    public final static String  ROOT_TAG       = "/production_line/tag";


    //  Pack Paths

    public final static String  BARRIER_PACK   = "/production_line/barrier_pack";

    public final static String  SIGNAL_PACK    = "/production_line/thread_signal/pack";

    public final static String  ROOT_PACK      = "/production_line/pack";


    //  Lock

    public final static String ROOT_LOCK        = "/production_line/locknode";

    
}
