package org.opencb.opencga.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;


import com.fasterxml.jackson.databind.SerializationFeature;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.db.CatalogManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.alignment.json.AlignmentDifferenceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.VariantFieldsProtos;
import org.opencb.opencga.storage.core.variant.io.json.ArchivedVariantFileJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantSourceJsonMixin;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.*;
import javax.ws.rs.core.Response.ResponseBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@Path("/")
public class OpenCGAWSServer {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected static Properties properties;
    protected static Config config;

    protected String version;
    protected UriInfo uriInfo;
    protected String sessionIp;

    // Common input arguments
    protected MultivaluedMap<String, String> params;
    protected QueryOptions queryOptions;
    protected QueryResponse queryResponse;

    // Common output members
    protected long startTime;
    protected long endTime;

    protected static ObjectWriter jsonObjectWriter;
    protected static ObjectMapper jsonObjectMapper;

    //Common query params
    @DefaultValue("")
    @QueryParam("sid")
    protected String sessionId;

    @DefaultValue("json")
    @QueryParam("of")
    protected String outputFormat;

    @DefaultValue("")
    @QueryParam("exclude")
    protected String exclude;

    @DefaultValue("")
    @QueryParam("include")
    protected String include;

    @DefaultValue("true")
    @QueryParam("metadata")
    protected Boolean metadata;

    protected static CatalogManager catalogManager;

    static {

        InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("catalog.properties");
        properties = new Properties();
        try {
            properties.load(is);
            System.out.println("catalog.properties");
            System.out.println(properties.getProperty("CATALOG.HOST"));
            System.out.println(properties.getProperty("CATALOG.PORT"));
            System.out.println(properties.getProperty("CATALOG.DATABASE"));
            System.out.println(properties.getProperty("CATALOG.USER"));
            System.out.println(properties.getProperty("CATALOG.PASSWORD"));
            System.out.println(properties.getProperty("ROOTDIR"));
        } catch (IOException e) {
            System.out.println("Error loading properties");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
//        InputStream is = OpenCGAWSServer.class.getClassLoader().getResourceAsStream("application.properties");
//        properties = new Properties();
//        try {
//            properties.load(is);
//        } catch (IOException e) {
//            System.out.println("Error loading properties");
//            System.out.println(e.getMessage());
//            e.printStackTrace();
//        }
//        Config.setGcsaHome(properties.getProperty("OPENCGA.INSTALLATION.DIR"));    //TODO: Check instalation dir.
//        properties = Config.getCatalogProperties();

        try {
            catalogManager = new CatalogManager(properties);
        } catch (IOException | CatalogIOManagerException | CatalogManagerException e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        jsonObjectMapper = new ObjectMapper();

        jsonObjectMapper.addMixInAnnotations(ArchivedVariantFile.class, ArchivedVariantFileJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantSource.class, VariantSourceJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
        jsonObjectMapper.addMixInAnnotations(Alignment.AlignmentDifference.class, AlignmentDifferenceJsonMixin.class);
        
        jsonObjectWriter = jsonObjectMapper.writer();

    }

    public OpenCGAWSServer(@PathParam("version") String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException {
        this.startTime = System.currentTimeMillis();
        this.version = version;
        this.uriInfo = uriInfo;
        logger.debug(uriInfo.getRequestUri().toString());

        this.queryOptions = new QueryOptions();
        queryOptions.put("exclude", exclude);
        queryOptions.put("include", include);
        queryOptions.put("metadata", metadata);

        this.sessionIp = httpServletRequest.getRemoteAddr();
    }


    protected Response createErrorResponse(Object o) {
        QueryResult<ObjectMap> result = new QueryResult();
        result.setErrorMsg(o.toString());
        return createOkResponse(result);
    }

    protected Response createOkResponse(Object obj) {
        queryResponse = new QueryResponse();
        endTime = System.currentTimeMillis() - startTime;
        queryResponse.setTime(new Long(endTime - startTime).intValue());
        queryResponse.setApiVersion(version);
        queryResponse.setQueryOptions(queryOptions);

        // Guarantee that the QueryResponse object contains a coll of results
        Collection coll;
        if (obj instanceof Collection) {
            coll = (Collection) obj;
        } else {
            coll = new ArrayList();
            coll.add(obj);
        }
        queryResponse.setResponse(coll);

        switch (outputFormat.toLowerCase()) {
            case "json":
                return createJsonResponse(queryResponse);
            case "xml":
//                return createXmlResponse(queryResponse);
            default:
                return buildResponse(Response.ok());
        }


    }

    protected Response createJsonResponse(Object object) {
        try {
            return buildResponse(Response.ok(jsonObjectWriter.writeValueAsString(object), MediaType.APPLICATION_JSON_TYPE));
        } catch (JsonProcessingException e) {
            System.out.println("object = " + object);
            System.out.println("((QueryResponse)object).getResponse() = " + ((QueryResponse) object).getResponse());

            System.out.println("e = " + e);
            System.out.println("e.getMessage() = " + e.getMessage());
            return createErrorResponse("Error parsing QueryResponse object:\n" + Arrays.toString(e.getStackTrace()));
        }
    }

    //Response methods
    protected Response createOkResponse(Object o1, MediaType o2) {
        return buildResponse(Response.ok(o1, o2));
    }

    protected Response createOkResponse(Object o1, MediaType o2, String fileName) {
        return buildResponse(Response.ok(o1, o2).header("content-disposition", "attachment; filename =" + fileName));
    }

    protected Response buildResponse(ResponseBuilder responseBuilder) {
        return responseBuilder.header("Access-Control-Allow-Origin", "*").header("Access-Control-Allow-Headers", "x-requested-with, content-type").build();
    }
}
