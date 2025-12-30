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
  CSVGenerationRequest
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
  }
};

