/*
 * SPDX-FileCopyrightText: syuilo and other misskey contributors
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { Inject, Injectable } from '@nestjs/common';
import { Brackets, And, In, MoreThan, Not } from 'typeorm';
import { DI } from '@/di-symbols.js';
import type { NotesRepository, UserNotePiningsRepository, UsersRepository } from '@/models/_.js';
import type Logger from '@/logger.js';
import type { MiNote } from '@/models/Note.js';
import { bindThis } from '@/decorators.js';
import { NoteDeleteService } from '@/core/NoteDeleteService.js';
import { QueueLoggerService } from '../QueueLoggerService.js';
import type * as Bull from 'bullmq';
import type { DbUserTruncateJobData } from '../types.js';

@Injectable()
export class TruncateAccountKeepDriveProcessorService {
	private logger: Logger;

	constructor(
		@Inject(DI.usersRepository)
		private usersRepository: UsersRepository,

		@Inject(DI.notesRepository)
		private notesRepository: NotesRepository,

		@Inject(DI.userNotePiningsRepository)
		private userNotePiningsRepository: UserNotePiningsRepository,

		private queueLoggerService: QueueLoggerService,
		private noteDeleteService: NoteDeleteService,
	) {
		this.logger = this.queueLoggerService.logger.createSubLogger('truncate-account-keep-drive');
	}

	@bindThis
	public async process(job: Bull.Job<DbUserTruncateJobData>): Promise<string | void> {
		this.logger.info(`Truncate notes (keep drive) of ${job.data.user.id} ...`);

		const user = await this.usersRepository.findOneBy({ id: job.data.user.id });
		if (user == null) {
			return;
		}

		const pinings = await this.userNotePiningsRepository.findBy({ userId: user.id });
		const piningNoteIds = pinings.map(pining => pining.noteId);
		const cascadingPiningNoteIds = piningNoteIds.length !== 0 ? await this.findCascadingNotes(piningNoteIds) : [];

		const specifiedNotes = await this.notesRepository.findBy({
			userId: user.id,
			visibility: Not(In(['public', 'home', 'followers'])),
		});
		const specifiedNoteIds = specifiedNotes.map(note => note.id);
		const cascadingSpecifiedNoteIds = specifiedNoteIds.length !== 0 ? await this.findCascadingNotes(specifiedNoteIds) : [];

		let cursor: MiNote['id'] | null = null;
		while (true) {
			const notes = await this.notesRepository.find({
				where: {
					userId: user.id,
					...(cursor ? {
						id: And(Not(In([...piningNoteIds, ...cascadingPiningNoteIds, ...specifiedNoteIds, ...cascadingSpecifiedNoteIds])), MoreThan(cursor)),
					} : {
						id: Not(In([...piningNoteIds, ...cascadingPiningNoteIds, ...specifiedNoteIds, ...cascadingSpecifiedNoteIds])),
					}),
				},
				take: 100,
				order: { id: 1 },
			}) as MiNote[];

			if (notes.length === 0) break;

			cursor = notes.at(-1)?.id ?? null;

			await Promise.all(notes.map((note) => this.noteDeleteService.delete(user, note, false, user)));
		}

		this.logger.succ('All of notes deleted (drive kept)');
		return 'Account notes truncated (drive kept)';
	}

	@bindThis
	private async findCascadingNotes(noteIds: MiNote['id'][]): Promise<MiNote['id'][]> {
		const recursive = async (noteIds: MiNote['id'][]): Promise<MiNote['id'][]> => {
			const query = this.notesRepository.createQueryBuilder('note')
				.where('note.replyId IN(:...noteIds)', { noteIds })
				.orWhere(new Brackets(q => {
					q.where('note.renoteId IN(:...noteIds)', { noteIds })
						.andWhere('note.text IS NOT NULL');
				}));
			const replies = await query.getMany();

			return [
				...replies.map((reply) => reply.id),
				...await Promise.all(replies.map(reply => recursive([reply.id]))),
			].flat();
		};

		const cascadingNotes: MiNote['id'][] = await recursive(noteIds);

		return cascadingNotes;
	}
}

