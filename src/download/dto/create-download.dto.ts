import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { IsIn, IsInt, IsNotEmpty, IsOptional, IsString, IsUrl, Min } from 'class-validator';
import { createDownloadDtoSwagger } from './create-download.swagger';

export class CreateDownloadDto {
  @ApiProperty(createDownloadDtoSwagger.url)
  @IsUrl()
  @IsNotEmpty()
  url: string;

  @ApiPropertyOptional(createDownloadDtoSwagger.title)
  @IsOptional()
  @IsString()
  title?: string;

  @ApiPropertyOptional(createDownloadDtoSwagger.p)
  @IsOptional()
  @IsInt()
  @Min(1)
  p?: number;

  @ApiPropertyOptional(createDownloadDtoSwagger.platform)
  @IsOptional()
  @IsIn(['auto', 'bilibili', 'youtube', 'generic'])
  platform?: 'auto' | 'bilibili' | 'youtube' | 'generic';

  @ApiPropertyOptional(createDownloadDtoSwagger.media)
  @IsOptional()
  @IsIn(['both', 'video', 'audio'])
  media?: 'both' | 'video' | 'audio';
}
