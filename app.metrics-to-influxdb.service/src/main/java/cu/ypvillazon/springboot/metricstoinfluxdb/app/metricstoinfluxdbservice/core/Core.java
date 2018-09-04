package cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.core;

import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.configuration.Applications;
import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.configuration.Service;
import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.discovery.IDiscoveryService;
import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.exceptions.EHttpError;
import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.exceptions.EJsonBad;
import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.influxdb.IInfluxDbService;
import cu.ypvillazon.springboot.metricstoinfluxdb.app.metricstoinfluxdbservice.restclient.IRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.net.URI;
import java.util.*;

/**
 * Created by ups on 14/01/18.
 */

@org.springframework.stereotype.Service
public class Core  {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${infludb.metrics}")
    private String[] metricsToMonitor ;

    @Autowired
    private IInfluxDbService influxDbService ;

    @Autowired
    private IDiscoveryService iDiscoveryService ;

    @Autowired
    private IRestClient iRestClient ;


    private Applications applications;

    @Autowired
    public void setApp(Applications applications) {
        this.applications = applications;
    }

    //增加GC统计信息
    //Parallel GC模式 （jdk1.8 默认垃圾收集器Parallel Scavenge（新生代）+Parallel Old（老年代））
    private final static Map<String,String> GC_ParallelMap=getGCMetricsList("young.gc.count:gc.ps_scavenge.count|young.gc.time:gc.ps_scavenge.time|old.gc.count:gc.ps_marksweep.count|old.gc.time:gc.ps_marksweep.time");
    //G1 GC模式  默认：(老年代使用G1)（新生代使用G1）
    private final static Map<String,String> GC_G1Map=getGCMetricsList("young.gc.count:gc.g1_young_generation.count|young.gc.time:gc.g1_young_generation.time|old.gc.count:gc.g1_old_generation.count|old.gc.time:gc.g1_old_generation.time");
    //CMS GC模式 默认：(老年代使用CMS)（新生代使用ParNew）
    private final static Map<String,String> GC_CMSMap=getGCMetricsList("young.gc.count:gc.parnew.count|young.gc.time:gc.parnew.time|old.gc.count:gc.concurrentmarksweep.count|old.gc.time:gc.concurrentmarksweep.time");

    private static Map<String,String> getGCMetricsList(String metrics) {
        List<String> metricsList= Arrays.asList(metrics.split("\\|"));
        Map<String,String>metricsMap=new HashMap<>();
        for (String item:metricsList) {
            String[] itemArr=item.split(":");
            metricsMap.put(itemArr[1].trim(),itemArr[0].trim());
        }
        return metricsMap;
    }

    @Scheduled(cron = "*/10 * * * * *")
    public void run(){
        if(applications.getServices() != null){
            for (Service service: applications.getServices()){
                URI uri = iDiscoveryService.getUri(service.getServiceId()) ;
                if(uri != null){
                    HashMap<String,String> tags = new HashMap<>() ;
                    tags.put("serviceId",service.getServiceId()) ;
                    tags.put("serviceName",service.getName()) ;
                    tags.put("serviceDescription",service.getDescription()) ;

                    HashMap<String,Object> fields = new HashMap<>() ;
                    try {
                        HashMap<String, Long> metrics = iRestClient.get(uri + "/metrics",HashMap.class) ;
                        if(metrics != null && metrics.size()>0){
                            Set set = metrics.entrySet() ;
                            Iterator iterator = set.iterator();
                            while(iterator.hasNext()) {
                                Map.Entry mentry = (Map.Entry)iterator.next();
                                if(Arrays.asList(metricsToMonitor).contains(mentry.getKey())){
                                    fields.put((String) mentry.getKey(), mentry.getValue()) ;
                                    continue;
                                }
                                GCMetricsInfo gcMetricsInfo= getGCMetricsInfo(mentry.getKey().toString());
                                if(gcMetricsInfo!=null){
                                    fields.put(gcMetricsInfo.getMetricsName(),mentry.getValue());
                                    fields.put("java.gc.type",gcMetricsInfo.getMetricsType());
                                }
                            }
                        }
                        influxDbService.writeMetrics(tags,fields);
                    } catch (EHttpError eHttpError) {
                        logger.error(eHttpError.getMessage());
                    } catch (EJsonBad eJsonBad) {
                        logger.error(eJsonBad.getMessage());
                    }
                }
            }
        }
    }
    //增加GC统计信息
    private GCMetricsInfo getGCMetricsInfo(String key) {
        String metricsName=null;
        metricsName=GC_ParallelMap.get(key);
        if(metricsName!=null){return new GCMetricsInfo(1,metricsName);}
        metricsName=GC_CMSMap.get(key);
        if(metricsName!=null){return new GCMetricsInfo(2,metricsName);}
        metricsName=GC_G1Map.get(key);
        if(metricsName!=null){return new GCMetricsInfo(3,metricsName);}
        return null;
    }
    class GCMetricsInfo{
        //GC 类型
        private Integer metricsType;
        //GC 指标名
        private String metricsName;

        public GCMetricsInfo(Integer metricsType,String metricsName) {
            this.metricsType = metricsType;
            this.metricsName=metricsName;
        }

        public Integer getMetricsType() {
            return metricsType;
        }

        public String getMetricsName() {
            return metricsName;
        }
    }
}
