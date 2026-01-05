import axios from 'axios';
import type {
  ProcessStartRequest,
  ProcessStartResponse,
  SuggestionsRequest,
  SuggestionsResponse,
  SaveChangesRequest,
  SaveChangesResponse,
  DistributionMetadataResponse,
  ToyDataGenerationRequest,
  CSVGenerationRequest,
  CheckpointResponse,
  CheckpointProceedRequest,
  CheckpointProceedResponse,
  DomainEditRequest,
  EntitiesEditRequest,
  RelationsEditRequest,
  AttributesEditRequest,
  PrimaryKeysEditRequest,
  MultivaluedDerivedEditRequest,
  NullabilityEditRequest,
  ERDiagramEditRequest,
  DatatypesEditRequest,
  RelationalSchemaEditRequest,
  InformationMiningEditRequest,
  FunctionalDependenciesEditRequest,
  ConstraintsEditRequest,
  GenerationStrategiesEditRequest
} from '../types/api';
import { API_BASE_URL } from '../utils/constants';

const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
});

export const apiService = {
  async startProcessing(request: ProcessStartRequest): Promise<ProcessStartResponse> {
    const response = await apiClient.post<ProcessStartResponse>('/api/process/start', request);
    return response.data;
  },
  
  async getSuggestions(request: SuggestionsRequest): Promise<SuggestionsResponse> {
    const response = await apiClient.post<SuggestionsResponse>('/api/suggestions', request);
    return response.data;
  },
  
  async saveChanges(request: SaveChangesRequest): Promise<SaveChangesResponse> {
    const response = await apiClient.post<SaveChangesResponse>('/api/schema/save_changes', request);
    return response.data;
  },
  
  async getERDiagramImage(jobId: string, format: 'png' | 'svg' = 'png'): Promise<string> {
    const response = await apiClient.get(
      `/api/schema/er_diagram_image/${jobId}?format=${format}`,
      { responseType: 'blob' }
    );
    return URL.createObjectURL(response.data);
  },
  
  async getDistributionsMetadata(): Promise<DistributionMetadataResponse> {
    const response = await apiClient.get<DistributionMetadataResponse>('/api/schema/distributions/metadata');
    return response.data;
  },
  
  async generateToyData(request: ToyDataGenerationRequest): Promise<{ job_id: string; status: string }> {
    const response = await apiClient.post<{ job_id: string; status: string }>('/api/generate/toy_data', request);
    return response.data;
  },
  
  async generateCSV(request: CSVGenerationRequest): Promise<{ job_id: string; status: string }> {
    const response = await apiClient.post<{ job_id: string; status: string }>('/api/generate/csv', request);
    return response.data;
  },
  
  // Checkpoint endpoints
  async getCheckpoint(jobId: string): Promise<CheckpointResponse> {
    const response = await apiClient.get<CheckpointResponse>(`/api/checkpoint/${jobId}`);
    return response.data;
  },
  
  async proceedToNextCheckpoint(request: CheckpointProceedRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/proceed', request);
    return response.data;
  },
  
  async saveDomainEdit(request: DomainEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/domain/save', request);
    return response.data;
  },
  
  async saveEntitiesEdit(request: EntitiesEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/entities/save', request);
    return response.data;
  },
  
  async saveRelationsEdit(request: RelationsEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/relations/save', request);
    return response.data;
  },
  
  async saveAttributesEdit(request: AttributesEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/attributes/save', request);
    return response.data;
  },
  
  async savePrimaryKeysEdit(request: PrimaryKeysEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/primary_keys/save', request);
    return response.data;
  },
  
  async saveERDiagramEdit(request: ERDiagramEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/er_diagram/save', request);
    return response.data;
  },
  
  async saveDatatypesEdit(request: DatatypesEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/datatypes/save', request);
    return response.data;
  },
  
  async saveMultivaluedDerivedEdit(request: MultivaluedDerivedEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/multivalued_derived/save', request);
    return response.data;
  },
  
  async saveNullabilityEdit(request: NullabilityEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/nullability/save', request);
    return response.data;
  },
  
  async saveRelationalSchemaEdit(request: RelationalSchemaEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/relational_schema/save', request);
    return response.data;
  },
  
  async saveInformationMiningEdit(request: InformationMiningEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/information_mining/save', request);
    return response.data;
  },
  
  async saveFunctionalDependenciesEdit(request: FunctionalDependenciesEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/functional_dependencies/save', request);
    return response.data;
  },
  
  async saveConstraintsEdit(request: ConstraintsEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/constraints/save', request);
    return response.data;
  },
  
  async saveGenerationStrategiesEdit(request: GenerationStrategiesEditRequest): Promise<CheckpointProceedResponse> {
    const response = await apiClient.post<CheckpointProceedResponse>('/api/checkpoint/generation_strategies/save', request);
    return response.data;
  }
};

