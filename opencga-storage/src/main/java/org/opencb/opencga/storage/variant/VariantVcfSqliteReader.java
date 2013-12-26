package org.opencb.opencga.storage.variant;

import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantDataReader;
import org.opencb.commons.db.SqliteSingletonConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by parce on 12/12/13.
 */
public class VariantVcfSqliteReader implements VariantDataReader {

    private Statement stmt;
    private PreparedStatement pstmt;
    private SqliteSingletonConnection connection;
    private ResultSet allVariantsResultSet;

    public VariantVcfSqliteReader(String dbName) {
        this.stmt = null;
        this.pstmt = null;
        this.allVariantsResultSet = null;
        this.connection = new SqliteSingletonConnection(dbName);
    }

    public List<String> getSampleNames() {
//        String sql = "INSERT INTO sample_stats VALUES(?,?,?,?);";
//        VariantSingleSampleStats s;
//        String name;
//        boolean res = true;
//        try {
//            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(sql);
//
//            for (Map.Entry<String, VariantSingleSampleStats> entry : vcfSampleStat.getSamplesStats().entrySet()) {
//                s = entry.getValue();
//                name = entry.getKey();
//
//                pstmt.setString(1, name);
//                pstmt.setInt(2, s.getMendelianErrors());
//                pstmt.setInt(3, s.getMissingGenotypes());
//                pstmt.setInt(4, s.getHomozygotesNumber());
//                pstmt.execute();
//
//            }
//            SqliteSingletonConnection.getConnection().commit();
//            pstmt.close();
//        } catch (SQLException e) {
//            System.err.println("SAMPLE_STATS: " + e.getClass().getName() + ": " + e.getMessage());
//            res = false;
//        }
//        return res;
        List<String> sampleNames = new ArrayList<String>();
        String selectNamesSql = "SELECT name FROM sample;";
        try {
            pstmt = SqliteSingletonConnection.getConnection().prepareStatement(selectNamesSql);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                sampleNames.add(rs.getString("name"));
            }
            rs.close();
        } catch (SQLException e) {
            System.err.println("Error obtaining sample names: " + e.getClass().getName() + ": " + e.getMessage());
        }
        return sampleNames;
    }

    public String getHeader() {
        // TODO: implementar esto. Usar el header de Vcf4
        return "##VCF header";
    }

    @Override
    public boolean open() {
        return SqliteSingletonConnection.getConnection() != null;
    }

    @Override
    public boolean close() {
        boolean res = true;
        try {
            SqliteSingletonConnection.getConnection().close();
        } catch (SQLException e) {
            e.printStackTrace();
            res = false;
        }
        return res;
    }

    // TODO: son necesarios pre y post??
    public boolean pre() {
        return true;
    }
    public boolean post() {
        return true;
    }

    public VcfRecord read() {
        if (this.allVariantsResultSet == null) {
            // run the query that select all variants, and save the result set
            String selectVariantsSql = "SELECT * FROM variant;";
            try {
                pstmt = SqliteSingletonConnection.getConnection().prepareStatement(selectVariantsSql);
                this.allVariantsResultSet = pstmt.executeQuery();
            } catch (SQLException e) {
                System.err.println("Error creating all variants ResultSet: " + e.getClass().getName() + ": " + e.getMessage());
            }
        }
        // obtain the next variant from the result set
        VcfRecord vcfRecord = null;
        try {
            if (allVariantsResultSet.next()) {
                vcfRecord = new VcfRecord(allVariantsResultSet.getString("chromosome"),
                                          allVariantsResultSet.getInt("position"),
                                          allVariantsResultSet.getString("id"),
                                          allVariantsResultSet.getString("ref"),
                                          allVariantsResultSet.getString("alt"),
                                          allVariantsResultSet.getString("qual"),
                                          allVariantsResultSet.getString("filter"),
                                          allVariantsResultSet.getString("info"),
                                          allVariantsResultSet.getString("format"));
            } else {
                allVariantsResultSet.close();
            }
        } catch (SQLException e) {
            System.err.println("Error obtaining variant from result set: " + e.getClass().getName() + ": " + e.getMessage());
        }

        return vcfRecord;
    }

    public List<VcfRecord> read(int batchSize) {
        List<VcfRecord> listRecords = new ArrayList<>(batchSize);
        VcfRecord vcfRecord;
        int i = 0;

        while ((i < batchSize) && (vcfRecord = this.read()) != null) {
            // TODO: el vcf reader crea filtros a partir de la info del header del archivo
            // aqui de momento no ponemos filtros
//            if (vcfFilters != null && vcfFilters.size() > 0) {
//                if (andVcfFilters.apply(vcfRecord)) {
//                    //vcfRecord.setSampleIndex(vcf4.getSamples());
//                    listRecords.add(vcfRecord);
//                    i++;
//                }
//            } else {
//                //vcfRecord.setSampleIndex(vcf4.getSamples());
//                listRecords.add(vcfRecord);
//                i++;
//            }
            listRecords.add(vcfRecord);
            i++;
        }
        return listRecords;
    }

}
