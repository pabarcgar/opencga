package org.opencb.opencga.storage.core.alignment.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.biodata.models.feature.Region;
import org.opencb.commons.io.DataReader;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;
/**
 * Date 26/08/14.
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 *
 * This class reads CoverageFiles generated by AlignmentCoverajeJsonDataWriter
 * building empty AlignmentRegion with RegionCoverage and MeanCoverage.
 *
 * CoverageFileName     : <name>.coverage.json.gz
 * MeanCoverageFileName : <name>.mean-coverage.json.gz
 *
 */
public class AlignmentCoverageJsonDataReader implements DataReader<AlignmentRegion> {


    public static final int REGION_SIZE = 100000;
    private final String coverageFilename;
    private final String meanCoverageFilename;
    private InputStream coverageStream;
    private InputStream meanCoverageStream;
    private final JsonFactory factory;
    private final ObjectMapper jsonObjectMapper;
    private JsonParser coverageParser;
    private JsonParser meanCoverageParser;
    private MeanCoverage meanCoverage;
    private boolean readRegionCoverage = true;
    private boolean readMeanCoverage = true;
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(AlignmentCoverageJsonDataReader.class);

    public AlignmentCoverageJsonDataReader(String coverageFilename, String meanCoverageFilename) {
        this.coverageFilename = coverageFilename;
        this.meanCoverageFilename = meanCoverageFilename;
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(this.factory);
    }

    @Override
    public boolean open() {
        try {
            coverageStream = new FileInputStream(coverageFilename);
            if(coverageFilename.endsWith(".gz")){
                coverageStream = new GZIPInputStream(coverageStream);
            }

            meanCoverageStream = new FileInputStream(meanCoverageFilename);
            if (meanCoverageFilename.endsWith(".gz")) {
                meanCoverageStream = new GZIPInputStream(meanCoverageStream);
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return false;
        }

        return true;
    }

    @Override
    public boolean close() {
        try {
            coverageStream.close();
            meanCoverageStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean pre() {
        try {
            coverageParser = factory.createParser(coverageStream);
            meanCoverageParser = factory.createParser(meanCoverageStream);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean post() {
        return false;
    }

    private MeanCoverage readMeanCoverage(){
        MeanCoverage meanCoverage = null;
        try {
            meanCoverage = meanCoverageParser.readValueAs(MeanCoverage.class);
        } catch (IOException e) {
            //e.printStackTrace();
        }
        return meanCoverage;
    }

    public AlignmentRegion readElem(){
        RegionCoverage regionCoverage = null;
        List<MeanCoverage> meanCoverageList = null;
        String chromosome = null;
        long start = 0;
        long end   = 0;

        if(!readMeanCoverage && !readRegionCoverage) {
            return null;
        }
        if(readRegionCoverage){
            try {
                regionCoverage = coverageParser.readValueAs(RegionCoverage.class);
            } catch (IOException e) {
                //e.printStackTrace();
                return null;
            }
            chromosome = regionCoverage.getChromosome();
            start = regionCoverage.getStart();
            end = regionCoverage.getEnd();
        }

        if(readMeanCoverage) {
            if (meanCoverage == null) {
                meanCoverage = readMeanCoverage();
                if(meanCoverage == null){
                    return null;
                }
            }
            if(chromosome == null){
                chromosome = meanCoverage.getRegion().getChromosome();
                start = meanCoverage.getRegion().getStart();
                end = meanCoverage.getRegion().getStart()+ REGION_SIZE;
            }
            meanCoverageList = new LinkedList<>();
            while (meanCoverage != null) {
                Region region = meanCoverage.getRegion();
                if (!(region.getChromosome().equals(chromosome) && region.getStart() < end)) {
                    break;
                }
                meanCoverageList.add(meanCoverage);
                meanCoverage = readMeanCoverage();
            }
        }
        logger.info("Read : " + chromosome + ":" + start + "-" + end + ", length = " + (end - start));
        AlignmentRegion alignmentRegion = new AlignmentRegion(chromosome, start, end);
        alignmentRegion.setCoverage(regionCoverage);
        alignmentRegion.setMeanCoverage(meanCoverageList);

        return alignmentRegion;
    }

    @Override
    public List<AlignmentRegion> read() {
        return Arrays.asList(readElem());
    }

    @Override
    public List<AlignmentRegion> read(int batchSize) {
        List<AlignmentRegion> alignmentRegions = new LinkedList<>();
        for(int i = 0; i < batchSize; i++){
            AlignmentRegion alignmentRegion = readElem();
            if(alignmentRegion == null){
                return alignmentRegions;
            }
            alignmentRegions.add(alignmentRegion);
        }
        if(alignmentRegions.isEmpty()){
            return null;
        }
        return alignmentRegions;
    }


    public boolean isReadRegionCoverage() {
        return readRegionCoverage;
    }

    public void setReadRegionCoverage(boolean readRegionCoverage) {
        this.readRegionCoverage = readRegionCoverage;
    }

    public boolean isReadMeanCoverage() {
        return readMeanCoverage;
    }

    public void setReadMeanCoverage(boolean readMeanCoverage) {
        this.readMeanCoverage = readMeanCoverage;
    }
}
