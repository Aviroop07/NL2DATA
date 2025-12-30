import React from 'react';
import { Box, LinearProgress, Typography, Tooltip } from '@mui/material';
import { useAppStore } from '../../stores/useAppStore';

const QualityBar: React.FC = () => {
  const { qualityScore, qualityBreakdown } = useAppStore();
  
  const getColor = (score: number): string => {
    if (score <= 40) return 'error';
    if (score <= 70) return 'warning';
    if (score <= 85) return 'info';
    return 'success';
  };
  
  const tooltipText = `
    Domain: ${qualityBreakdown.domain}/20
    Entities: ${qualityBreakdown.entities}/25
    Attributes: ${qualityBreakdown.column_names}/20
    Cardinalities: ${qualityBreakdown.cardinalities}/15
    Constraints: ${qualityBreakdown.constraints}/10
    Relationships: ${qualityBreakdown.relationships}/10
  `;
  
  const getQualityLabel = (score: number): string => {
    if (score <= 40) return 'Needs Improvement';
    if (score <= 70) return 'Good';
    if (score <= 85) return 'Very Good';
    return 'Excellent';
  };

  return (
    <Box sx={{ mb: 2 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1.5 }}>
        <Box>
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
            Description Quality
          </Typography>
          <Typography variant="h6" sx={{ fontWeight: 700, color: `${getColor(qualityScore)}.main` }}>
            {qualityScore}/100
          </Typography>
        </Box>
        <Tooltip 
          title={
            <Box sx={{ p: 0.5 }}>
              <Typography variant="caption" sx={{ display: 'block', mb: 0.5, fontWeight: 600 }}>
                Quality Breakdown:
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Domain: {qualityBreakdown.domain}/20
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Entities: {qualityBreakdown.entities}/25
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Attributes: {qualityBreakdown.column_names}/20
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Cardinalities: {qualityBreakdown.cardinalities}/15
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Constraints: {qualityBreakdown.constraints}/10
              </Typography>
              <Typography variant="caption" sx={{ display: 'block' }}>
                Relationships: {qualityBreakdown.relationships}/10
              </Typography>
            </Box>
          }
          arrow
        >
          <Typography 
            variant="body2" 
            color="text.secondary" 
            sx={{ 
              cursor: 'help',
              textDecoration: 'underline',
              textDecorationStyle: 'dotted',
              textUnderlineOffset: 4
            }}
          >
            {getQualityLabel(qualityScore)}
          </Typography>
        </Tooltip>
      </Box>
      <LinearProgress
        variant="determinate"
        value={qualityScore}
        color={getColor(qualityScore) as any}
        sx={{ 
          height: 10, 
          borderRadius: 2,
          bgcolor: 'grey.200',
          '& .MuiLinearProgress-bar': {
            borderRadius: 2,
          }
        }}
      />
    </Box>
  );
};

export default QualityBar;

