import type { ExtractedItems, QualityBreakdown } from '../types/state';

export function calculateQualityScore(
  extractedItems: ExtractedItems | null
): { score: number; breakdown: QualityBreakdown } {
  if (!extractedItems) {
    return {
      score: 0,
      breakdown: {
        domain: 0,
        entities: 0,
        column_names: 0,
        cardinalities: 0,
        constraints: 0,
        relationships: 0
      }
    };
  }
  
  const breakdown: QualityBreakdown = {
    domain: extractedItems.domain ? 20 : 0,
    entities: calculateEntityScore(extractedItems.entities),
    column_names: calculateColumnScore(extractedItems.column_names),
    cardinalities: calculateCardinalityScore(extractedItems.cardinalities),
    constraints: extractedItems.constraints.length > 0 ? 10 : 0,
    relationships: extractedItems.relationships.length > 0 ? 10 : 0
  };
  
  const score = Object.values(breakdown).reduce((sum, val) => sum + val, 0);
  
  return { score, breakdown };
}

function calculateEntityScore(entities: string[]): number {
  if (entities.length >= 3) return 25;
  if (entities.length === 2) return 15;
  if (entities.length === 1) return 10;
  return 0;
}

function calculateColumnScore(columns: string[]): number {
  if (columns.length >= 5) return 20;
  if (columns.length >= 3) return 15;
  if (columns.length >= 1) return 10;
  return 0;
}

function calculateCardinalityScore(cardinalities: string[]): number {
  if (cardinalities.length >= 2) return 15;
  if (cardinalities.length === 1) return 10;
  return 0;
}

