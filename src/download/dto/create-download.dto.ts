import { ApiProperty } from '@nestjs/swagger';
import { IsInt, IsNotEmpty, IsOptional, IsString, IsUrl, Min } from 'class-validator';

export class CreateDownloadDto {
  @ApiProperty({
    description: 'Public video URL to download (Bilibili link or similar)',
    example: 'https://www.bilibili.com/video/BV1xx411c7mD',
    required: true,
  })
  @IsUrl()
  @IsNotEmpty()
  url: string;

  @ApiProperty({
    description: 'Optional title to save the video under',
    required: false,
    example: 'My favorite clip',
  })
  @IsOptional()
  @IsString()
  title?: string;

  @ApiProperty({
    description: 'Optional page/part number for multi-part Bilibili videos (1-based)',
    required: false,
    example: 2,
    minimum: 1,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  p?: number;
}
