# Development of SWOT Datasets for Ukraine: Technical Implementation Report

## Executive Summary

This report presents the technical implementation of Surface Water and Ocean Topography (SWOT) datasets for monitoring water levels of the Dnipro River in Ukraine, integrated within the IHP-WINS platform. The implementation includes automated data acquisition, processing, and visualization capabilities through TerriaJS integration.

## 1. Introduction

The Surface Water and Ocean Topography (SWOT) mission provides critical satellite-based measurements for monitoring inland water bodies. This project focuses on establishing a comprehensive data pipeline for SWOT Level 2 High Resolution River Single Pass (L2_HR_RiverSP) data specifically covering the Dnipro River basin in Ukraine. The implementation leverages Apache Airflow for workflow orchestration, CKAN for data management, and TerriaJS for interactive visualization.

## 2. SWOT Dataset Implementation

### 2.1 Dataset Configuration

The implementation establishes two distinct SWOT datasets within the IHP-WINS platform:

- **Node Dataset**: `swot-level-2-river-single-pass-vector-node-data-product-for-ukraine`
- **Reach Dataset**: `swot-level-2-river-single-pass-vector-reach-data-product-for-ukraine`

These datasets capture different aspects of river hydrology:
- **Node data**: Point-based measurements providing detailed water surface elevation at specific river locations
- **Reach data**: Linear features representing averaged water characteristics over river segments

### 2.2 Automated Data Acquisition

The system implements an automated data acquisition workflow using NASA's PO.DAAC (Physical Oceanography Distributed Active Archive Center) data subscriber:

```python
command = [
    'podaac-data-subscriber',
    '-c', 'SWOT_L2_HR_RiverSP_2.0',
    '-d', './data',
    '-m', '1200',
    '-e', '.zip',
    '-b', '22,44,40,52'  # Bounding box covering Ukraine region
]
```

**Key Features:**
- Scheduled execution every 10 hours using Airflow DAG
- Geographic filtering using bounding box coordinates (22°-40°E, 44°-52°N)
- Automated credential management through `.netrc` configuration
- Error handling and retry mechanisms for resilient data acquisition

### 2.3 Data Processing and Upload

The implementation includes intelligent file classification and upload mechanisms:

- **File Type Detection**: Automatic differentiation between Node and Reach data files
- **Duplicate Prevention**: API-based verification to prevent redundant uploads
- **Metadata Enhancement**: Automatic generation of descriptive metadata for each resource

## 3. TerriaJS Integration and Visualization

### 3.1 Interactive Map Visualization

The system integrates with TerriaJS to provide interactive visualization capabilities for SWOT data. Key visualization features include:

**Water Surface Elevation Mapping:**
- Continuous color mapping using "Warm" and "Reds" color palettes
- Dynamic range scaling (0-2100 meters elevation)
- Temporal animation capabilities for time-series analysis

**Temporal Controls:**
- Time-based data exploration using `time_str` column
- Configurable playback speed and timeline navigation
- Support for both point-based (Node) and linear (Reach) temporal visualization

### 3.2 Custom Configuration Management

The implementation includes automated configuration updates for TerriaJS viewers:

```python
view['custom_config'] = "https://ihp-wins.unesco.org/terria/#start=..."
```

This ensures that each dataset is properly configured with:
- Appropriate styling for water surface elevation visualization
- Correct temporal settings for time-series data
- Optimized camera positions focused on the Dnipro River region

## 4. Technical Architecture

### 4.1 Workflow Orchestration

The system utilizes Apache Airflow for:
- **Scheduled Execution**: 10-hour intervals for regular data updates
- **Error Handling**: Comprehensive retry mechanisms and error logging
- **Variable Management**: Secure storage of API keys and NASA credentials

### 4.2 API Integration

**CKAN Integration:**
- RESTful API communication for resource management
- Automatic resource creation and metadata assignment
- View configuration updates for visualization enhancements

**NASA EarthData Integration:**
- Secure authentication using `.netrc` credentials
- Automated data download from PO.DAAC repositories
- Geographic and temporal filtering capabilities

## 5. Data Quality and Monitoring Features

### 5.1 Quality Assurance

The implementation includes several quality assurance mechanisms:

- **Existence Verification**: Pre-upload checks to prevent data duplication
- **File Integrity**: Validation of downloaded data files
- **Metadata Consistency**: Automated generation of standardized metadata

### 5.2 Monitoring Capabilities

**Water Level Monitoring:**
- Continuous monitoring of Dnipro River water surface elevations
- Historical data preservation for trend analysis
- Real-time data availability through automated workflows

## 6. Remote Sensing Water Quality Integration (Planned)

While the current implementation focuses on SWOT elevation data, the architecture supports future integration of remote sensing water quality datasets for the Dnipro River. The modular design allows for:

- Extension of existing workflows to accommodate additional data sources
- Integration with optical and radar satellite data for water quality parameters
- Harmonization of temporal and spatial resolutions across different datasets

## 7. Spatio-Temporal Asset Catalog (STAC) Implementation Status

The implementation of a Spatio-Temporal Asset Catalog (STAC) for water quality data of the Dnipro River remains **pending due to external complications beyond the consultant's control**. These complications include:

- Infrastructure dependencies requiring third-party coordination
- Standardization requirements pending organizational approval
- Technical specifications awaiting finalization by external stakeholders

The current CKAN-based implementation provides interim cataloging capabilities while STAC development is resolved.

## 8. Time Series Retrieval Feature

The TerriaJS integration includes time series retrieval capabilities through:

### 8.1 Temporal Data Structure

- **Time Column Configuration**: Utilization of `time_str` field for temporal indexing
- **Unique Identifiers**: Use of `reach_id` for reach data and appropriate identifiers for node data
- **Temporal Spreading**: Automatic distribution of temporal events across visualization timeline

### 8.2 Interactive Features

- **Timeline Navigation**: User-controlled temporal exploration
- **Animation Controls**: Play/pause functionality with configurable speed
- **Data Point Selection**: Click-based selection for detailed temporal analysis

## 9. Conclusions and Recommendations

### 9.1 Achievements

The implementation successfully establishes:
- Automated SWOT data acquisition and processing pipeline
- Dual dataset structure for comprehensive river monitoring
- Interactive visualization capabilities through TerriaJS integration
- Robust error handling and quality assurance mechanisms

### 9.2 Future Enhancements

**Immediate Priorities:**
1. Resolution of STAC implementation constraints
2. Integration of water quality remote sensing datasets
3. Enhanced temporal analysis capabilities

**Long-term Objectives:**
1. Real-time data streaming capabilities
2. Predictive modeling integration
3. Multi-sensor data fusion for comprehensive water monitoring

### 9.3 Technical Recommendations

1. **Scalability**: Consider implementing horizontal scaling for processing larger temporal datasets
2. **Performance**: Optimize data transfer mechanisms for improved real-time capabilities
3. **Integration**: Develop standardized APIs for seamless third-party data source integration

## 10. Technical Specifications

**System Requirements:**
- Apache Airflow 2.x
- Python 3.8+
- NASA EarthData credentials
- CKAN API access
- TerriaJS compatible infrastructure

**Data Specifications:**
- Format: Shapefile (SHP) in ZIP archives
- Coordinate System: Geographic (WGS84)
- Temporal Resolution: Variable based on satellite passes
- Spatial Coverage: Ukraine region (22°-40°E, 44°-52°N)

This implementation provides a robust foundation for monitoring the Dnipro River's water levels and establishes the infrastructure necessary for future water quality monitoring enhancements. 